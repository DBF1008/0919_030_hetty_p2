package bolt

import (
	"context"
	"fmt"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

// Default batch writer settings. They deliberately favor low latency while
// still collapsing the many single-item transactions of highly concurrent
// request logging into a single transactional commit.
var (
	DefaultBatchInterval = 10 * time.Millisecond
	DefaultBatchSize     = 64
)

// maxBatchWriteRetries is the number of times a batch commit is retried when
// the underlying Bolt transaction fails with a transient error.
const maxBatchWriteRetries = 3

// writeQueueSize buffers incoming mutations so producers can finish enqueuing
// a size-based batch (then wait for its result) without deadlocking with the
// writer goroutine.
const writeQueueSize = 4096

// Database is used to store and retrieve data from an underlying Bolt database.
type Database struct {
	bolt *bolt.DB

	// Batch writer.
	writes    chan batchWrite
	stop      chan struct{}
	stopped   chan struct{}
	interval  time.Duration
	maxSize   int
	closeOnce sync.Once
}

// BatchConfig configures the background writer that batches database
// mutations into a single Bolt transaction.
type BatchConfig struct {
	// Interval is the maximum amount of time a queued mutation waits before
	// it is committed.
	Interval time.Duration
	// MaxSize is the maximum number of mutations committed in one batch.
	MaxSize int
}

// withDefaults fills in zero-valued settings with their defaults.
func (c BatchConfig) withDefaults() BatchConfig {
	if c.Interval <= 0 {
		c.Interval = DefaultBatchInterval
	}

	if c.MaxSize <= 0 {
		c.MaxSize = DefaultBatchSize
	}

	return c
}

// batchWrite is a single queued mutation. fn performs the mutation inside a
// write transaction. It must return an error only for transient transaction
// failures that warrant a whole-batch retry. Permanent, item-specific errors
// (such as a missing bucket or request) should be delivered via result, while
// letting the transaction continue, so they don't poison the rest of a batch.
type batchWrite struct {
	fn      func(*bolt.Tx) error
	result  chan error
	barrier bool
}

func newBarrier() batchWrite {
	return batchWrite{
		barrier: true,
		result:  make(chan error, 1),
		fn:      func(*bolt.Tx) error { return nil },
	}
}

// OpenDatabase opens a new Bolt database.
func OpenDatabase(path string, opts *bolt.Options) (*Database, error) {
	db, err := bolt.Open(path, 0o600, opts)
	if err != nil {
		return nil, fmt.Errorf("bolt: failed to open database: %w", err)
	}

	return DatabaseFromBoltDB(db)
}

// Close flushes any pending mutations and closes the underlying Bolt
// database. It is safe to call multiple times.
func (db *Database) Close() error {
	db.closeOnce.Do(func() {
		// Stop the background writer and wait for its final flush.
		close(db.stop)
		<-db.stopped
	})

	return db.bolt.Close()
}

// DatabaseFromBoltDB returns a Database with `db` set as the underlying Bolt
// database, using the default batch writer settings.
func DatabaseFromBoltDB(db *bolt.DB) (*Database, error) {
	return DatabaseFromBoltDBWithConfig(db, BatchConfig{})
}

// DatabaseFromBoltDBWithConfig returns a Database with `db` set as the
// underlying Bolt database and a batch writer configured with cfg.
func DatabaseFromBoltDBWithConfig(db *bolt.DB, cfg BatchConfig) (*Database, error) {
	cfg = cfg.withDefaults()

	database := &Database{
		bolt:     db,
		writes:   make(chan batchWrite, writeQueueSize),
		stop:     make(chan struct{}),
		stopped:  make(chan struct{}),
		interval: cfg.Interval,
		maxSize:  cfg.MaxSize,
	}

	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(projectsBucketName)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("bolt: failed to create projects bucket: %w", err)
	}

	go database.runBatchWriter()

	return database, nil
}

// enqueueWrite queues fn for execution inside a batched write transaction and
// blocks until that batch has been committed or ctx is canceled.
func (db *Database) enqueueWrite(ctx context.Context, fn func(*bolt.Tx) error) error {
	w := batchWrite{
		fn:     fn,
		result: make(chan error, 1),
	}

	select {
	case <-db.stop:
		return bolt.ErrDatabaseNotOpen
	default:
	}

	select {
	case db.writes <- w:
	case <-db.stop:
		return bolt.ErrDatabaseNotOpen
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-w.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Flush blocks until every mutation enqueued before the call has been
// committed (or permanently failed). It is primarily useful in tests and
// during a clean shutdown.
func (db *Database) Flush(ctx context.Context) error {
	barrier := newBarrier()

	select {
	case <-db.stop:
		return bolt.ErrDatabaseNotOpen
	default:
	}

	select {
	case db.writes <- barrier:
	case <-db.stop:
		return bolt.ErrDatabaseNotOpen
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-barrier.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// runBatchWriter is the background goroutine that aggregates mutations and
// commits them in a single transaction whenever the batch interval elapses or
// the maximum batch size is reached.
func (db *Database) runBatchWriter() {
	defer close(db.stopped)

	var batch []batchWrite

	commit := func() {
		if len(batch) == 0 {
			return
		}

		db.commitBatch(batch)
		batch = batch[:0]
	}

	timer := time.NewTimer(db.interval)
	defer timer.Stop()

	for {
		select {
		case w := <-db.writes:
			batch = append(batch, w)

			// A barrier (Flush) or a full batch commits immediately.
			if w.barrier || len(batch) >= db.maxSize {
				commit()

				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(db.interval)
			}
		case <-timer.C:
			commit()
			timer.Reset(db.interval)
		case <-db.stop:
			// Drain any mutations that were already queued so a clean
			// shutdown doesn't drop buffered request/response logs.
			for {
				select {
				case w := <-db.writes:
					batch = append(batch, w)
				default:
					commit()
					return
				}
			}
		}
	}
}

// commitBatch runs every mutation in one transaction, retrying the entire
// batch with exponential backoff on transient transaction failures.
func (db *Database) commitBatch(batch []batchWrite) {
	var txErr error

	for attempt := 0; attempt <= maxBatchWriteRetries; attempt++ {
		// Item-specific errors are captured (not sent yet) and don't abort
		// — and thus don't roll back — the whole batch. They are delivered to
		// callers only after the transaction commits successfully.
		results := make([]error, len(batch))
		txErr = db.bolt.Update(func(tx *bolt.Tx) error {
			for i, w := range batch {
				results[i] = w.fn(tx)
			}

			return nil
		})
		if txErr == nil {
			for i, w := range batch {
				w.result <- results[i]
			}

			return
		}

		// Backoff before retrying the whole batch.
		if attempt < maxBatchWriteRetries {
			backoff := time.Duration(1<<uint(attempt)) * time.Millisecond
			timer := time.NewTimer(backoff)
			select {
			case <-timer.C:
			case <-db.stop:
				timer.Stop()
			}
		}
	}

	// Retries are exhausted. Report the transaction error for every item so
	// callers can log/handle the permanent failure.
	for _, w := range batch {
		w.result <- fmt.Errorf("bolt: failed to commit batch after %d retries: %w", maxBatchWriteRetries, txErr)
	}
}

func createNestedBucket(tx *bolt.Tx, names ...[]byte) (b *bolt.Bucket, err error) {
	for i, name := range names {
		if b == nil {
			b, err = tx.CreateBucketIfNotExists(name)
		} else {
			b, err = b.CreateBucketIfNotExists(name)
		}
		if err != nil {
			return nil, fmt.Errorf("bolt: failed to create nested bucket %q: %w", names[:i+1], err)
		}
	}

	return b, nil
}
