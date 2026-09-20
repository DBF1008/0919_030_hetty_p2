package bolt_test

import (
	"context"
	"errors"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/oklog/ulid"
	bbolt "go.etcd.io/bbolt"

	"github.com/dstotijn/hetty/pkg/db/bolt"
	"github.com/dstotijn/hetty/pkg/proj"
	"github.com/dstotijn/hetty/pkg/reqlog"
)

func newTestDatabase(t *testing.T, cfg *bolt.BatchConfig) *bolt.Database {
	t.Helper()

	path := t.TempDir() + "bolt.db"
	boltDB, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("failed to open bolt database: %v", err)
	}

	var db *bolt.Database
	if cfg != nil {
		db, err = bolt.DatabaseFromBoltDBWithConfig(boltDB, *cfg)
	} else {
		db, err = bolt.DatabaseFromBoltDB(boltDB)
	}

	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("failed to close database: %v", err)
		}
	})

	return db
}

func newProject(t *testing.T, db *bolt.Database) ulid.ULID {
	t.Helper()

	projectID := ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy)
	if err := db.UpsertProject(context.Background(), proj.Project{ID: projectID}); err != nil {
		t.Fatalf("unexpected error upserting project: %v", err)
	}

	return projectID
}

func newReqLog(t *testing.T, projectID ulid.ULID) reqlog.RequestLog {
	t.Helper()

	return reqlog.RequestLog{
		ID:        ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy),
		ProjectID: projectID,
		URL:       mustParseURL(t, "https://example.com/"),
		Method:    "GET",
		Proto:     "HTTP/1.1",
	}
}

func TestStoreRequestLogBatching(t *testing.T) {
	t.Parallel()

	// A long interval lets us assert that stores block until an explicit
	// flush drives the batch (i.e. writes are genuinely buffered, not
	// committed per call).
	cfg := bolt.BatchConfig{Interval: time.Hour, MaxSize: 64}
	db := newTestDatabase(t, &cfg)
	projectID := newProject(t, db)

	const n = 10
	logs := make([]reqlog.RequestLog, n)
	for i := range logs {
		logs[i] = newReqLog(t, projectID)
	}

	stored := make(chan error, n)

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()
			stored <- db.StoreRequestLog(context.Background(), logs[i])
		}(i)
	}

	// Before a flush, none of the buffered stores may complete.
	select {
	case err := <-stored:
		t.Fatalf("store completed before flush (expected buffering), err: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	// Force the queued mutations out in a single batch; all stores unblock.
	if err := db.Flush(context.Background()); err != nil {
		t.Fatalf("failed to flush: %v", err)
	}

	wg.Wait()
	close(stored)

	for err := range stored {
		if err != nil {
			t.Fatalf("unexpected error storing request log: %v", err)
		}
	}

	got, err := db.FindRequestLogs(context.Background(), reqlog.FindRequestsFilter{ProjectID: projectID}, nil)
	if err != nil {
		t.Fatalf("failed to find request logs: %v", err)
	}

	if len(got) != n {
		t.Fatalf("expected %d stored logs, got %d", n, len(got))
	}
}

func TestStoreRequestLogBatchBySize(t *testing.T) {
	t.Parallel()

	// Size-based flush should commit without waiting for the interval. A
	// size batch is filled by concurrent producers: each enqueues and then
	// waits for the batch result.
	const n = 8
	cfg := bolt.BatchConfig{Interval: time.Hour, MaxSize: n}
	db := newTestDatabase(t, &cfg)
	projectID := newProject(t, db)

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			if err := db.StoreRequestLog(context.Background(), newReqLog(t, projectID)); err != nil {
				t.Errorf("unexpected error storing request log: %v", err)
			}
		}()
	}
	wg.Wait()

	got, err := db.FindRequestLogs(context.Background(), reqlog.FindRequestsFilter{ProjectID: projectID}, nil)
	if err != nil {
		t.Fatalf("failed to find request logs: %v", err)
	}

	if len(got) != n {
		t.Fatalf("expected %d stored logs, got %d", n, len(got))
	}
}

func TestStoreRequestLogConcurrentSingleTransactionBatch(t *testing.T) {
	t.Parallel()

	cfg := bolt.BatchConfig{Interval: 5 * time.Millisecond, MaxSize: 128}
	db := newTestDatabase(t, &cfg)
	projectID := newProject(t, db)

	const n = 100
	logs := make([]reqlog.RequestLog, n)
	for i := range logs {
		logs[i] = newReqLog(t, projectID)
	}

	var wg sync.WaitGroup
	for i := range logs {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()
			if err := db.StoreRequestLog(context.Background(), logs[i]); err != nil {
				t.Errorf("unexpected error storing request log: %v", err)
			}
		}(i)
	}
	wg.Wait()

	if err := db.Flush(context.Background()); err != nil {
		t.Fatalf("failed to flush: %v", err)
	}

	got, err := db.FindRequestLogs(context.Background(), reqlog.FindRequestsFilter{ProjectID: projectID}, nil)
	if err != nil {
		t.Fatalf("failed to find request logs: %v", err)
	}

	if len(got) != n {
		t.Fatalf("expected %d stored logs, got %d", n, len(got))
	}
}

func TestStoreResponseLogBatched(t *testing.T) {
	t.Parallel()

	cfg := bolt.BatchConfig{Interval: 5 * time.Millisecond, MaxSize: 64}
	db := newTestDatabase(t, &cfg)
	projectID := newProject(t, db)

	reqLog := newReqLog(t, projectID)
	if err := db.StoreRequestLog(context.Background(), reqLog); err != nil {
		t.Fatalf("unexpected error storing request log: %v", err)
	}

	resLog := reqlog.ResponseLog{
		Proto:      "HTTP/1.1",
		StatusCode: 200,
		Status:     "200 OK",
	}

	if err := db.StoreResponseLog(context.Background(), projectID, reqLog.ID, resLog); err != nil {
		t.Fatalf("unexpected error storing response log: %v", err)
	}

	got, err := db.FindRequestLogByID(context.Background(), projectID, reqLog.ID)
	if err != nil {
		t.Fatalf("failed to find request log: %v", err)
	}

	if got.Response == nil || got.Response.StatusCode != 200 {
		t.Fatalf("expected response log to be stored, got: %#v", got.Response)
	}
}

func TestStoreResponseLogRequestNotFound(t *testing.T) {
	t.Parallel()

	cfg := bolt.BatchConfig{Interval: time.Millisecond, MaxSize: 64}
	db := newTestDatabase(t, &cfg)
	projectID := newProject(t, db)

	missingID := ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy)
	err := db.StoreResponseLog(context.Background(), projectID, missingID, reqlog.ResponseLog{})
	if !errors.Is(err, reqlog.ErrRequestNotFound) {
		t.Fatalf("expected reqlog.ErrRequestNotFound, got: %v", err)
	}

	// A failing item must not poison other items in the same batch: store a
	// valid request log afterwards and ensure it succeeds.
	reqLog := newReqLog(t, projectID)
	if err := db.StoreRequestLog(context.Background(), reqLog); err != nil {
		t.Fatalf("unexpected error storing request log: %v", err)
	}

	if _, err := db.FindRequestLogByID(context.Background(), projectID, reqLog.ID); err != nil {
		t.Fatalf("expected valid request log to be committed, got: %v", err)
	}
}

func TestClearRequestLogsBatched(t *testing.T) {
	t.Parallel()

	cfg := bolt.BatchConfig{Interval: 5 * time.Millisecond, MaxSize: 64}
	db := newTestDatabase(t, &cfg)
	projectID := newProject(t, db)

	const n = 5
	for i := 0; i < n; i++ {
		if err := db.StoreRequestLog(context.Background(), newReqLog(t, projectID)); err != nil {
			t.Fatalf("unexpected error storing request log: %v", err)
		}
	}

	if err := db.ClearRequestLogs(context.Background(), projectID); err != nil {
		t.Fatalf("unexpected error clearing request logs: %v", err)
	}

	got, err := db.FindRequestLogs(context.Background(), reqlog.FindRequestsFilter{ProjectID: projectID}, nil)
	if err != nil {
		t.Fatalf("failed to find request logs: %v", err)
	}

	if len(got) != 0 {
		t.Fatalf("expected no logs after clear, got %d", len(got))
	}

	// Bucket is recreated, so logging keeps working after a clear.
	reqLog := reqlog.RequestLog{
		ID:        ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy),
		ProjectID: projectID,
		URL:       &url.URL{Scheme: "https", Host: "example.com"},
		Method:    "GET",
		Proto:     "HTTP/1.1",
	}
	if err := db.StoreRequestLog(context.Background(), reqLog); err != nil {
		t.Fatalf("unexpected error storing request log after clear: %v", err)
	}

	if _, err := db.FindRequestLogByID(context.Background(), projectID, reqLog.ID); err != nil {
		t.Fatalf("expected request log to be stored after clear, got: %v", err)
	}
}

func TestFlushAfterClose(t *testing.T) {
	t.Parallel()

	db := newTestDatabase(t, nil)
	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}

	if err := db.Flush(context.Background()); !errors.Is(err, bbolt.ErrDatabaseNotOpen) {
		t.Fatalf("expected bolt.ErrDatabaseNotOpen, got: %v", err)
	}
}
