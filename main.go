package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"relay-gateway/audit"
	"relay-gateway/config"
	"relay-gateway/db"
	"relay-gateway/media"
	"relay-gateway/migration"
	"relay-gateway/profilebootstrap"
	"relay-gateway/router"
	"relay-gateway/security"
	"relay-gateway/service"
	"relay-gateway/task"
)

func main() {
	configFile := "config.yaml"
	if err := config.Load(configFile); err != nil {
		log.Fatalf("Fatal: load configuration %s failed: %v", configFile, err)
	}

	// SQLite is the only durable source of channels, credentials and request logs.
	dbPath := config.GetDatabasePath()
	if err := db.InitDB(dbPath); err != nil {
		log.Fatalf("Init SQLite database %s failed: %v", dbPath, err)
	}
	log.Printf("SQLite database initialized at: %s", dbPath)
	if report, err := profilebootstrap.EnsureBuiltinProfiles(context.Background()); err != nil {
		// Built-in profile materialization is additive and must not prevent the
		// legacy gateway from starting when an operator-owned id collides.
		log.Printf("Warning: materialize builtin protocol profiles failed: %v", err)
	} else if len(report.Created) > 0 {
		log.Printf("Materialized builtin protocol profiles: %s", strings.Join(report.Created, ", "))
	}
	if report, err := router.BackfillLegacyTaskLifecycle(context.Background()); err != nil {
		// Historical mappings are compatibility data. Startup remains available
		// for the legacy gateway when one malformed row needs operator review.
		log.Printf("Warning: backfill legacy task lifecycle failed: %v", err)
	} else if report.Projected > 0 || report.Skipped > 0 {
		log.Printf("Backfilled legacy task lifecycle mappings: scanned=%d projected=%d already_ready=%d skipped=%d", report.Scanned, report.Projected, report.AlreadyReady, report.Skipped)
	}

	defer db.Close()

	if len(os.Args) >= 3 && os.Args[1] == "admin" && os.Args[2] == "reset" {
		fmt.Print("This removes the administrator and every admin session. Type RESET to continue: ")
		answer, _ := bufio.NewReader(os.Stdin).ReadString('\n')
		if strings.TrimSpace(answer) != "RESET" {
			log.Println("Admin reset cancelled.")
			return
		}
		if err := security.ResetAdmin(); err != nil {
			log.Fatalf("Reset administrator failed: %v", err)
		}
		log.Println("Administrator reset. Open /setup locally to create a new account.")
		return
	}

	if len(os.Args) >= 3 && os.Args[1] == "migrate" && os.Args[2] == "bindings" {
		report, err := router.ReconcileAllChannelsProfileBindings(context.Background(), true)
		if err != nil {
			log.Fatalf("Reconcile channel profile bindings failed: %v", err)
		}
		log.Printf("Reconciled bindings for %d channels: %s (skipped: %s)", len(report.ReconciledChannels), strings.Join(report.ReconciledChannels, ", "), strings.Join(report.SkippedChannels, ", "))
		auditReport, err := migration.AuditChannels(context.Background())
		if err != nil {
			log.Fatalf("Audit channels migration failed: %v", err)
		}
		if auditReport.Ready {
			log.Printf("Migration audit PASSED: All %d channels are ready for RELAY_DISABLE_LEGACY=1", auditReport.EnabledChannels)
		} else {
			log.Printf("Migration audit status: ready=%v, blockers=%s", auditReport.Ready, strings.Join(auditReport.Blockers, "; "))
		}
		return
	}

	if pStr := db.GetSetting("port", ""); pStr != "" {
		if p, err := strconv.Atoi(pStr); err == nil && config.IsValidPort(p) {
			config.SetPort(p)
		} else {
			log.Printf("Ignoring invalid persisted port %q; using configured port %d", pStr, config.GetPort())
		}
	}
	workerCtx, stopWorkers := context.WithCancel(context.Background())
	// Sample SQLite size periodically so growth evidence is available without
	// exposing database contents or credentials through the metrics endpoint.
	metricsDone := make(chan struct{})
	go func() {
		defer close(metricsDone)
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		sample := func() {
			if err := router.RuntimeMetrics.SampleDatabase(dbPath); err != nil {
				log.Printf("[Metrics] database sample failed: %v", err)
			}
		}
		sample()
		for {
			select {
			case <-workerCtx.Done():
				return
			case <-ticker.C:
				sample()
			}
		}
	}()
	cleanupDone := audit.StartCleanupWorker(workerCtx)
	taskMappingRecoveryDone := router.StartAsyncTaskMappingRecoveryWorker(workerCtx)
	profilePollerDone := make(chan struct{})
	go func() {
		defer close(profilePollerDone)
		if err := task.NewProfileBackgroundPoller("gateway-profile").Start(workerCtx); err != nil && err != context.Canceled {
			log.Printf("[ProfilePoller] stopped: %v", err)
		}
	}()
	mediaMaterializerDone := make(chan struct{})
	mediaDeletionDone := make(chan struct{})
	mediaRetentionDone := make(chan struct{})
	mediaReconcileDone := make(chan struct{})
	mediaRoot := filepath.Dir(db.DatabasePath())
	if strings.TrimSpace(mediaRoot) == "." || strings.TrimSpace(mediaRoot) == "" {
		mediaRoot = filepath.Dir(config.GetDatabasePath())
	}
	if mediaStore, mediaErr := media.NewLocalObjectStore(filepath.Join(mediaRoot, "media"), int64(512<<20)); mediaErr != nil {
		log.Printf("[MediaWorkers] disabled: %v", mediaErr)
		close(mediaMaterializerDone)
		close(mediaDeletionDone)
		close(mediaRetentionDone)
		close(mediaReconcileDone)
	} else {
		migrateLegacyMediaFiles(filepath.Join(mediaRoot, "media", "profile"))
		go func() {
			defer close(mediaMaterializerDone)
			if err := task.NewMediaMaterializationPoller("gateway-media", mediaStore).Start(workerCtx); err != nil && err != context.Canceled {
				log.Printf("[MediaMaterializer] stopped: %v", err)
			}
		}()
		go func() {
			defer close(mediaDeletionDone)
			if err := task.NewMediaDeletionPoller("gateway-media-delete", mediaStore).Start(workerCtx); err != nil && err != context.Canceled {
				log.Printf("[MediaDeletion] stopped: %v", err)
			}
		}()
		go func() {
			defer close(mediaRetentionDone)
			scanner := &task.MediaRetentionScanner{}
			// Run once on startup, then periodically. Expiration only revokes
			// links and queues the existing deletion worker; it never fetches
			// provider content or deletes files inline.
			if report, err := scanner.Scan(workerCtx); err != nil {
				log.Printf("[MediaRetention] startup scan failed: %v", err)
			} else if report.Expired > 0 {
				log.Printf("[MediaRetention] startup expired %d/%d assets", report.Expired, report.Scanned)
			}
			ticker := time.NewTicker(15 * time.Minute)
			defer ticker.Stop()
			for {
				select {
				case <-workerCtx.Done():
					return
				case <-ticker.C:
					report, err := scanner.Scan(workerCtx)
					if err != nil {
						log.Printf("[MediaRetention] scan failed: %v", err)
					} else if report.Expired > 0 {
						log.Printf("[MediaRetention] expired %d/%d assets", report.Expired, report.Scanned)
					}
				}
			}
		}()
		go func() {
			defer close(mediaReconcileDone)
			poller := task.NewMediaReconcilePoller(mediaStore)
			poller.DeleteOrphans = os.Getenv("RELAY_MEDIA_DELETE_ORPHANS") == "1"
			if err := poller.Start(workerCtx); err != nil && err != context.Canceled {
				log.Printf("[MediaReconcile] stopped: %v", err)
			}
		}()
	}
	defer func() {
		stopWorkers()
		<-metricsDone
		<-cleanupDone
		<-taskMappingRecoveryDone
		<-profilePollerDone
		<-mediaMaterializerDone
		<-mediaDeletionDone
		<-mediaRetentionDone
		<-mediaReconcileDone
	}()

	// 3. 预热本地数据库已缓存的模型列表，避免冷启动瞬态空窗
	service.DefaultDispatcher.LoadCachedModelsFromDB()

	// 4. 启动时异步拉取所有开启渠道的模型刷新
	service.DefaultDispatcher.SyncRemoteModels(context.Background())

	r := router.Setup()

	port := config.GetPort()
	addr := fmt.Sprintf("0.0.0.0:%d", port)
	log.Printf("==========================================================")
	if security.HasAdmin() {
		log.Printf("  Relay Gateway Web 控制台: http://localhost:%d/login", port)
	} else {
		log.Printf("  Relay Gateway 首次设置:   http://localhost:%d/setup", port)
	}
	log.Printf("  OpenAI 兼容端点:        http://localhost:%d/v1", port)
	log.Printf("  Anthropic 兼容端点:     http://localhost:%d/v1/messages", port)
	log.Printf("  SQLite 调用日志页面:     http://localhost:%d/logs", port)
	log.Printf("==========================================================")

	srv := &http.Server{
		Addr:              addr,
		Handler:           r,
		ReadTimeout:       60 * time.Second,
		ReadHeaderTimeout: 10 * time.Second,
		// Long-running streaming responses are bounded at the connection level;
		// request concurrency is separately capped by the router middleware.
		WriteTimeout:   15 * time.Minute,
		IdleTimeout:    120 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed to start: %v", err)
		}
	}()

	// 优雅停机监听系统退出信号 (SIGINT / SIGTERM)
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit
	log.Println("[Server] Shutting down Relay Gateway server...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("[Server] Forced shutdown: %v", err)
	}
	stopWorkers()
	<-metricsDone
	<-profilePollerDone
	<-mediaMaterializerDone
	<-mediaDeletionDone
	<-mediaRetentionDone
	<-mediaReconcileDone
	<-taskMappingRecoveryDone
	<-cleanupDone
	if err := service.DefaultDispatcher.StopSync(shutdownCtx); err != nil {
		log.Printf("[Server] Model sync did not stop cleanly: %v", err)
	}
	log.Println("[Server] Relay Gateway exited cleanly.")
}

func migrateLegacyMediaFiles(profileDir string) {
	entries, err := os.ReadDir(profileDir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".media") {
			continue
		}
		oldName := entry.Name()
		newName := strings.TrimSuffix(oldName, ".media") + ".mp4"
		oldPath := filepath.Join(profileDir, oldName)
		newPath := filepath.Join(profileDir, newName)
		if err := os.Rename(oldPath, newPath); err == nil {
			oldKey := "profile/" + oldName
			newKey := "profile/" + newName
			if db.DB != nil {
				if err := db.DB.Model(&db.MediaObject{}).Where("storage_key = ?", oldKey).Update("storage_key", newKey).Error; err != nil {
					log.Printf("[MediaStore] DB update failed for %s -> %s: %v, reverting file rename", oldName, newName, err)
					_ = os.Rename(newPath, oldPath)
					continue
				}
			}
			log.Printf("[MediaStore] Migrated legacy media file: %s -> %s", oldName, newName)
		}
	}

	// Reconcile interrupted migrations where the disk file was already renamed to .mp4
	// but the process was interrupted before the database storage_key could be updated.
	if db.DB != nil {
		var legacyObjects []db.MediaObject
		if err := db.DB.Where("storage_key LIKE 'profile/%.media'").Find(&legacyObjects).Error; err == nil {
			for _, obj := range legacyObjects {
				oldName := strings.TrimPrefix(obj.StorageKey, "profile/")
				newName := strings.TrimSuffix(oldName, ".media") + ".mp4"
				oldPath := filepath.Join(profileDir, oldName)
				newPath := filepath.Join(profileDir, newName)
				if _, oldErr := os.Stat(oldPath); os.IsNotExist(oldErr) {
					if _, newErr := os.Stat(newPath); newErr == nil {
						newKey := "profile/" + newName
						if err := db.DB.Model(&db.MediaObject{}).Where("id = ?", obj.ID).Update("storage_key", newKey).Error; err == nil {
							log.Printf("[MediaStore] Reconciled interrupted migration: %s -> %s", oldName, newName)
						}
					}
				}
			}
		}
	}
}
