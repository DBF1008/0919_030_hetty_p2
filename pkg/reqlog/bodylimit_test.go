package reqlog_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/oklog/ulid"
	bbolt "go.etcd.io/bbolt"

	"github.com/dstotijn/hetty/pkg/db/bolt"
	"github.com/dstotijn/hetty/pkg/proj"
	"github.com/dstotijn/hetty/pkg/proxy"
	"github.com/dstotijn/hetty/pkg/reqlog"
	"github.com/dstotijn/hetty/pkg/scope"
)

const tinyBodyLimit = 4

func newBodyLimitService(t *testing.T, maxBodySize int64) (*reqlog.Service, *bolt.Database) {
	t.Helper()

	path := t.TempDir() + "bolt.db"
	boltDB, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("failed to open bolt database: %v", err)
	}

	db, err := bolt.DatabaseFromBoltDBWithConfig(boltDB, bolt.BatchConfig{
		Interval: time.Millisecond,
		MaxSize:  64,
	})
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("failed to close database: %v", err)
		}
	})

	projectID := ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy)
	if err := db.UpsertProject(context.Background(), proj.Project{ID: projectID}); err != nil {
		t.Fatalf("unexpected error upserting project: %v", err)
	}

	svc := reqlog.NewService(reqlog.Config{
		Repository:  db,
		Scope:       &scope.Scope{},
		MaxBodySize: maxBodySize,
	})
	svc.SetActiveProjectID(projectID)

	return svc, db
}

//nolint:paralleltest
func TestRequestModifierBodyLimit(t *testing.T) {
	svc, _ := newBodyLimitService(t, tinyBodyLimit)

	next := func(req *http.Request) {}
	reqModFn := svc.RequestModifier(next)

	// Body exceeds the 4-byte log cap.
	fullBody := "this is a much larger request body than the cap"
	req := httptest.NewRequest("POST", "https://example.com/", strings.NewReader(fullBody))
	reqID := ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy)
	req = req.WithContext(proxy.WithRequestID(req.Context(), reqID))

	reqModFn(req)

	t.Run("forwarded request body is not truncated", func(t *testing.T) {
		got, err := io.ReadAll(req.Body)
		if err != nil {
			t.Fatalf("failed to read forwarded body: %v", err)
		}

		if string(got) != fullBody {
			t.Fatalf("forwarded body mismatch (expected: %q, got: %q)", fullBody, string(got))
		}
	})

	t.Run("stored log body is capped", func(t *testing.T) {
		got, err := svc.FindRequestLogByID(context.Background(), reqID)
		if err != nil {
			t.Fatalf("failed to find request log: %v", err)
		}

		if int64(len(got.Body)) != tinyBodyLimit {
			t.Fatalf("expected stored body length %d, got %d (%q)", tinyBodyLimit, len(got.Body), string(got.Body))
		}
	})
}

//nolint:paralleltest
func TestRequestModifierSmallBodyUnderLimit(t *testing.T) {
	svc, _ := newBodyLimitService(t, tinyBodyLimit)

	reqModFn := svc.RequestModifier(func(req *http.Request) {})

	req := httptest.NewRequest("GET", "https://example.com/", strings.NewReader("abc"))
	reqID := ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy)
	req = req.WithContext(proxy.WithRequestID(req.Context(), reqID))

	reqModFn(req)

	got, err := svc.FindRequestLogByID(context.Background(), reqID)
	if err != nil {
		t.Fatalf("failed to find request log: %v", err)
	}

	if string(got.Body) != "abc" {
		t.Fatalf("expected stored body %q, got %q", "abc", string(got.Body))
	}
}

//nolint:paralleltest
func TestResponseModifierBodyLimit(t *testing.T) {
	svc, db := newBodyLimitService(t, tinyBodyLimit)

	reqLogID := ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy)
	if err := db.StoreRequestLog(context.Background(), reqlog.RequestLog{
		ID:        reqLogID,
		ProjectID: svc.ActiveProjectID(),
	}); err != nil {
		t.Fatalf("failed to store request log: %v", err)
	}

	resModFn := svc.ResponseModifier(func(res *http.Response) error { return nil })

	fullBody := "this is a much larger response body than the cap"
	req := httptest.NewRequest("GET", "https://example.com/", nil)
	req = req.WithContext(context.WithValue(req.Context(), reqlog.ReqLogIDKey, reqLogID))
	res := &http.Response{
		Request: req,
		Body:    io.NopCloser(strings.NewReader(fullBody)),
	}

	if err := resModFn(res); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	t.Run("client response body is not truncated", func(t *testing.T) {
		got, err := io.ReadAll(res.Body)
		if err != nil {
			t.Fatalf("failed to read response body: %v", err)
		}

		if string(got) != fullBody {
			t.Fatalf("response body mismatch (expected: %q, got: %q)", fullBody, string(got))
		}
	})

	t.Run("stored response log body is capped", func(t *testing.T) {
		// The response is stored from a background goroutine; poll (flushing
		// each time) until it has been committed.
		var got reqlog.RequestLog

		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if err := db.Flush(context.Background()); err != nil {
				t.Fatalf("failed to flush: %v", err)
			}

			var err error
			got, err = svc.FindRequestLogByID(context.Background(), reqLogID)
			if err != nil {
				t.Fatalf("failed to find request log: %v", err)
			}

			if got.Response != nil {
				break
			}

			time.Sleep(5 * time.Millisecond)
		}

		if got.Response == nil {
			t.Fatal("expected response log to be stored")
		}

		if int64(len(got.Response.Body)) != tinyBodyLimit {
			t.Fatalf("expected stored response body length %d, got %d (%q)",
				tinyBodyLimit, len(got.Response.Body), string(got.Response.Body))
		}
	})
}
