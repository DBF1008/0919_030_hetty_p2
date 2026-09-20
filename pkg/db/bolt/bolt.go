package bolt

import (
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/dstotijn/hetty/pkg/log"
)

// Default settings for the batch writer, which buffers write operations and
// commits them in a single Bolt transaction.
const (
	defaultMaxBatchSize   = 64
	defaultFlushInterval  = 100 * time.Millisecond
	defaultMaxRetries     = 3
	defaultRetryBackoff   = 50 * time.Millisecond
	batchOpBufferCapacity = 1024
)

// Database is used to store and retrieve data from an underlying Bolt database.
type Database struct {
	bolt *bolt.DB
	bw   *batchWriter
}

// OpenDatabase opens a new Bolt database.
func OpenDatabase(path string, opts *bolt.Options) (*Database, error) {
	db, err := bolt.Open(path, 0o600, opts)
	if err != nil {
		return nil, fmt.Errorf("bolt: failed to open database: %w", err)
	}

	return DatabaseFromBoltDB(db)
}

// Close closes the underlying Bolt database.
func (db *Database) Close() error {
	// Stop the batch writer first, so pending buffered writes are flushed
	// before the underlying Bolt database is closed.
	db.bw.close()

	return db.bolt.Close()
}

// Flush blocks until all write operations submitted so far have been
// committed to the underlying Bolt database.
func (db *Database) Flush() {
	done := make(chan struct{})

	select {
	case db.bw.ops <- batchOp{done: done}:
		<-done
	case <-db.bw.stopped:
	}
}

// SetBatchWriterLogger sets the logger used by the batch writer for
// reporting failed batch commits. It must be called before the database is
// used concurrently.
func (db *Database) SetBatchWriterLogger(logger log.Logger) {
	db.bw.cfg.logger = logger
}

// DatabaseFromBoltDB returns a Database with `db` set as the underlying Bolt
// database.
func DatabaseFromBoltDB(db *bolt.DB) (*Database, error) {
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

	database := &Database{bolt: db}
	database.bw = newBatchWriter(db, batchWriterConfig{
		maxBatchSize:  defaultMaxBatchSize,
		flushInterval: defaultFlushInterval,
		maxRetries:    defaultMaxRetries,
		retryBackoff:  defaultRetryBackoff,
		logger:        log.NewNopLogger(),
	})

	return database, nil
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

// batchOp is a single write operation that gets executed inside a batched
// Bolt transaction. If `done` is non-nil, the op acts as a barrier: the
// channel is closed once all preceding ops have been committed.
type batchOp struct {
	fn   func(tx *bolt.Tx) error
	done chan struct{}
}

type batchWriterConfig struct {
	maxBatchSize  int
	flushInterval time.Duration
	maxRetries    int
	retryBackoff  time.Duration
	logger        log.Logger
}

// batchWriter buffers write operations and commits them in a single Bolt
// transaction, either when the batch is full or a flush interval elapses.
// This avoids serializing every incoming log write on BoltDB's single
// read-write transaction lock under high concurrency.
type batchWriter struct {
	db     *bolt.DB
	cfg    batchWriterConfig
	ops    chan batchOp
	stop   chan struct{}
	stopped chan struct{}
}

func newBatchWriter(db *bolt.DB, cfg batchWriterConfig) *batchWriter {
	bw := &batchWriter{
		db:      db,
		cfg:     cfg,
		ops:     make(chan batchOp, batchOpBufferCapacity),
		stop:    make(chan struct{}),
		stopped: make(chan struct{}),
	}

	go bw.run()

	return bw
}

// submit buffers a write operation for the next batched commit. It blocks
// when the buffer is full, applying backpressure to callers.
func (bw *batchWriter) submit(fn func(tx *bolt.Tx) error) {
	bw.ops <- batchOp{fn: fn}
}

// close flushes pending operations and stops the batch writer.
func (bw *batchWriter) close() {
	close(bw.stop)
	<-bw.stopped
}

func (bw *batchWriter) run() {
	defer close(bw.stopped)

	var pending []batchOp

	timer := time.NewTimer(bw.cfg.flushInterval)
	defer timer.Stop()

	resetTimer := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}

		timer.Reset(bw.cfg.flushInterval)
	}

	for {
		select {
		case op := <-bw.ops:
			if op.done != nil {
				// Barrier op: commit everything buffered so far.
				bw.flushOps(pending)
				pending = nil
				resetTimer()
				close(op.done)

				continue
			}

			pending = append(pending, op)

			if len(pending) >= bw.cfg.maxBatchSize {
				bw.flushOps(pending)
				pending = nil
				resetTimer()
			}
		case <-timer.C:
			if len(pending) > 0 {
				bw.flushOps(pending)
				pending = nil
			}

			timer.Reset(bw.cfg.flushInterval)
		case <-bw.stop:
			// Drain remaining buffered ops before shutting down.
			for draining := true; draining; {
				select {
				case op := <-bw.ops:
					if op.done != nil {
						bw.flushOps(pending)
						pending = nil
						close(op.done)

						continue
					}

					pending = append(pending, op)
				default:
					draining = false
				}
			}

			bw.flushOps(pending)

			return
		}
	}
}

// flushOps commits a batch of operations in a single Bolt transaction.
// Failed commits are retried with backoff; if all retries are exhausted the
// batch is dropped and an error is logged.
func (bw *batchWriter) flushOps(ops []batchOp) {
	if len(ops) == 0 {
		return
	}

	var err error

	for attempt := 0; attempt <= bw.cfg.maxRetries; attempt++ {
		err = bw.db.Update(func(tx *bolt.Tx) error {
			for _, op := range ops {
				if err := op.fn(tx); err != nil {
					return err
				}
			}

			return nil
		})
		if err == nil {
			return
		}

		bw.cfg.logger.Infow("Failed to commit batched writes, retrying.",
			"attempt", attempt+1,
			"error", err)

		time.Sleep(bw.cfg.retryBackoff * time.Duration(attempt+1))
	}

	bw.cfg.logger.Errorw("Failed to commit batched writes after retries, dropping batch.",
		"error", err,
		"droppedOps", len(ops))
}
