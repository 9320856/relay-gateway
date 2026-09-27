package db

import (
	"context"
	"errors"
	"strings"
	"time"

	"gorm.io/gorm"
)

// WithTaskRunLeaseContext fences a complete group of durable poll mutations.
// Checking the lease in the same transaction as result/media writes prevents
// an expired response from overwriting the worker that took over the task.
func WithTaskRunLeaseContext(ctx context.Context, id, owner string, apply func(context.Context) error) error {
	conn, err := taskDB(ctx)
	if err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	id, owner = strings.TrimSpace(id), strings.TrimSpace(owner)
	if id == "" || owner == "" || apply == nil {
		return errors.New("task lease identity and callback are required")
	}
	transactionConn := conn
	if HasContextTransaction(ctx) {
		// A caller may give its retained transaction a shorter operation
		// deadline. Keep SAVEPOINT rollback on the owner's live context;
		// business SQL still observes the operation deadline below.
		transactionConn = DBForContext(ctx)
	}
	return transactionConn.Transaction(func(tx *gorm.DB) error {
		tx = tx.WithContext(ctx)
		var run TaskRun
		if err := tx.Where("id = ? AND lease_owner = ? AND lease_expires_at > ?", id, owner, time.Now()).First(&run).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrTaskLeaseOwner
			}
			return err
		}
		return apply(WithTx(ctx, tx))
	})
}

func RenewTaskRunLeaseContext(ctx context.Context, id, owner string, lease time.Duration) error {
	conn, err := taskDB(ctx)
	if err != nil {
		return err
	}
	if lease <= 0 || strings.TrimSpace(id) == "" || strings.TrimSpace(owner) == "" {
		return ErrTaskLeaseOwner
	}
	now := time.Now()
	result := conn.Model(&TaskRun{}).Where("id = ? AND lease_owner = ? AND lease_expires_at > ?", id, owner, now).Update("lease_expires_at", now.Add(lease))
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return ErrTaskLeaseOwner
	}
	return nil
}

func taskLeaseOwner(owner string) (string, error) {
	// Use the same bounded random fencing identity as durable media jobs.
	return mediaMaterializationOwner(owner)
}
