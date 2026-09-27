package audit

import (
	"context"
	"errors"
	"testing"
	"time"

	"relay-gateway/db"
)

func TestAuditAdmissionAndReadsCancelWhileDatabaseBusy(t *testing.T) {
	initAuditTestDB(t)
	pool, err := db.DB.DB()
	if err != nil {
		t.Fatal(err)
	}
	reserved, err := pool.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer reserved.Close()
	checks := []struct {
		name string
		run  func(context.Context) error
	}{
		{"admission", func(ctx context.Context) error {
			_, err := StartContext(ctx, "api_call", "test", "GET", "/v1/models", nil)
			return err
		}},
		{"list", func(ctx context.Context) error { _, _, err := ListContext(ctx, ListQuery{}); return err }},
		{"detail", func(ctx context.Context) error { _, err := GetDetailContext(ctx, "missing"); return err }},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- check.run(ctx) }()
			select {
			case err := <-done:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("query ignored deadline: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("canceled query still waits for sole database connection")
			}
		})
	}
}
