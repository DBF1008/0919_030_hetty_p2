package bolt

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"

	"github.com/oklog/ulid"
	bolt "go.etcd.io/bbolt"

	"github.com/dstotijn/hetty/pkg/reqlog"
	"github.com/dstotijn/hetty/pkg/scope"
)

var ErrRequestLogsBucketNotFound = errors.New("bolt: request logs bucket not found")

var reqLogsBucketName = []byte("request_logs")

func requestLogsBucket(tx *bolt.Tx, projectID ulid.ULID) (*bolt.Bucket, error) {
	pb, err := projectBucket(tx, projectID[:])
	if err != nil {
		return nil, err
	}

	b := pb.Bucket(reqLogsBucketName)
	if b == nil {
		return nil, ErrRequestLogsBucketNotFound
	}

	return b, nil
}

func (db *Database) FindRequestLogs(ctx context.Context, filter reqlog.FindRequestsFilter, scope *scope.Scope) (reqLogs []reqlog.RequestLog, err error) {
	if filter.ProjectID.Compare(ulid.ULID{}) == 0 {
		return nil, reqlog.ErrProjectIDMustBeSet
	}

	tx, err := db.bolt.Begin(false)
	if err != nil {
		return nil, fmt.Errorf("bolt: failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	b, err := requestLogsBucket(tx, filter.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("bolt: failed to get request logs bucket: %w", err)
	}

	err = b.ForEach(func(reqLogID, rawReqLog []byte) error {
		var reqLog reqlog.RequestLog
		err = gob.NewDecoder(bytes.NewReader(rawReqLog)).Decode(&reqLog)
		if err != nil {
			return fmt.Errorf("failed to decode request log: %w", err)
		}

		if filter.OnlyInScope && !reqLog.MatchScope(scope) {
			return nil
		}

		// Filter by search expression. TODO: Once pagination is introduced,
		// this filter logic should be done as items are retrieved.
		if filter.SearchExpr != nil {
			match, err := reqLog.Matches(filter.SearchExpr)
			if err != nil {
				return fmt.Errorf("failed to match search expression for request log (id: %v): %w", reqLogID, err)
			}

			if !match {
				return nil
			}
		}

		reqLogs = append(reqLogs, reqLog)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("bolt: failed to iterate over request logs: %w", err)
	}

	// Reverse items, so newest requests appear first.
	for i, j := 0, len(reqLogs)-1; i < j; i, j = i+1, j-1 {
		reqLogs[i], reqLogs[j] = reqLogs[j], reqLogs[i]
	}

	return reqLogs, nil
}

func (db *Database) FindRequestLogByID(ctx context.Context, projectID, reqLogID ulid.ULID) (reqLog reqlog.RequestLog, err error) {
	err = db.bolt.View(func(tx *bolt.Tx) error {
		b, err := requestLogsBucket(tx, projectID)
		if err != nil {
			return fmt.Errorf("bolt: failed to get request logs bucket: %w", err)
		}
		rawReqLog := b.Get(reqLogID[:])
		if rawReqLog == nil {
			return reqlog.ErrRequestNotFound
		}

		err = gob.NewDecoder(bytes.NewReader(rawReqLog)).Decode(&reqLog)
		if err != nil {
			return fmt.Errorf("failed to decode request log: %w", err)
		}

		return nil
	})
	if err != nil {
		return reqlog.RequestLog{}, fmt.Errorf("bolt: failed to find request log by ID: %w", err)
	}

	return reqLog, nil
}

// StoreRequestLog queues the request log for inclusion in a batched write
// transaction. It blocks until the batch containing the log has been
// committed (with retries on transient failures), preserving synchronous
// durability for callers while avoiding a separate transaction per request.
func (db *Database) StoreRequestLog(ctx context.Context, reqLog reqlog.RequestLog) error {
	buf := bytes.Buffer{}

	err := gob.NewEncoder(&buf).Encode(reqLog)
	if err != nil {
		return fmt.Errorf("bolt: failed to encode request log: %w", err)
	}

	rawReqLog := append([]byte(nil), buf.Bytes()...)

	err = db.enqueueWrite(ctx, func(txn *bolt.Tx) error {
		b, err := requestLogsBucket(txn, reqLog.ProjectID)
		if err != nil {
			return fmt.Errorf("failed to get request logs bucket: %w", err)
		}

		if err := b.Put(reqLog.ID[:], rawReqLog); err != nil {
			return fmt.Errorf("failed to put request log: %w", err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("bolt: failed to commit batch transaction: %w", err)
	}

	return nil
}

// StoreResponseLog queues the response log for inclusion in a batched write
// transaction. The batch writer retries transient transaction failures, so
// temporary Bolt errors no longer cause an asynchronous response store to be
// lost silently.
func (db *Database) StoreResponseLog(ctx context.Context, projectID, reqLogID ulid.ULID, resLog reqlog.ResponseLog) error {
	buf := bytes.Buffer{}

	err := gob.NewEncoder(&buf).Encode(resLog)
	if err != nil {
		return fmt.Errorf("bolt: failed to encode response log: %w", err)
	}

	err = db.enqueueWrite(ctx, func(txn *bolt.Tx) error {
		b, err := requestLogsBucket(txn, projectID)
		if err != nil {
			return fmt.Errorf("failed to get request logs bucket: %w", err)
		}

		rawReqLog := b.Get(reqLogID[:])
		if rawReqLog == nil {
			return reqlog.ErrRequestNotFound
		}

		var reqLog reqlog.RequestLog
		err = gob.NewDecoder(bytes.NewReader(rawReqLog)).Decode(&reqLog)
		if err != nil {
			return fmt.Errorf("failed to decode request log: %w", err)
		}

		reqLog.Response = &resLog

		encodedReqLog := bytes.Buffer{}
		err = gob.NewEncoder(&encodedReqLog).Encode(reqLog)
		if err != nil {
			return fmt.Errorf("failed to encode request log: %w", err)
		}

		err = b.Put(reqLog.ID[:], encodedReqLog.Bytes())
		if err != nil {
			return fmt.Errorf("failed to put request log: %w", err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("bolt: failed to commit batch transaction: %w", err)
	}

	return nil
}

// ClearRequestLogs first flushes any buffered writes so nothing outlives the
// clear operation, then synchronously deletes (and recreates) the request
// logs bucket.
func (db *Database) ClearRequestLogs(ctx context.Context, projectID ulid.ULID) error {
	if err := db.Flush(ctx); err != nil {
		return fmt.Errorf("bolt: failed to flush batched writes: %w", err)
	}

	err := db.enqueueWrite(ctx, func(txn *bolt.Tx) error {
		pb, err := projectBucket(txn, projectID[:])
		if err != nil {
			return fmt.Errorf("failed to get project bucket: %w", err)
		}

		// Tolerate a missing bucket so clearing is idempotent.
		if err := pb.DeleteBucket(reqLogsBucketName); err != nil && !errors.Is(err, bolt.ErrBucketNotFound) {
			return fmt.Errorf("failed to delete request logs bucket: %w", err)
		}

		if _, err := pb.CreateBucketIfNotExists(reqLogsBucketName); err != nil {
			return fmt.Errorf("failed to recreate request logs bucket: %w", err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("bolt: failed to commit batch transaction: %w", err)
	}

	return nil
}
