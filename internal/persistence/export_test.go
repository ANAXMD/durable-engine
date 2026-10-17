package persistence

import (
	"fmt"
)

// ExpireLeaseForTest is a test-only helper that artificially sets a workflow's
// run_at and lease_expires_at to the past, allowing the next poll to reclaim
// it without waiting the full 1-minute TTL.
func (d *DB) ExpireLeaseForTest(workflowID string) error {
	const query = `
		UPDATE workflows
		SET
			run_at           = NOW() - INTERVAL '1 second',
			lease_expires_at = NOW() - INTERVAL '1 second'
		WHERE id = $1
	`
	_, err := d.sqlDB.Exec(query, workflowID)
	if err != nil {
		return fmt.Errorf("failed to expire lease for test: %w", err)
	}
	return nil
}
