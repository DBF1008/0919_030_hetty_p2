package reqlog_test

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/oklog/ulid"
	"go.etcd.io/bbolt"

	"github.com/dstotijn/hetty/pkg/db/bolt"
	"github.com/dstotijn/hetty/pkg/proj"
	"github.com/dstotijn/hetty/pkg/proxy"
	"github.com/dstotijn/hetty/pkg/reqlog"
	"github.com/dstotijn/hetty/pkg/scope"
)

//nolint:gosec
var ulidEntropy = rand.New(rand.NewSource(time.Now().UnixNano()))

//nolint:paralleltest
func TestRequestModifier(t *testing.T) {
	path := t.TempDir() + "bolt.db"
	boltDB, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("failed to open bolt database: %v", err)
	}
	defer boltDB.Close()

	db, err := bolt.DatabaseFromBoltDB(boltDB)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	projectID := ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy)
	err = db.UpsertProject(context.Background(), proj.Project{
		ID: projectID,
	})
	if err != nil {
		t.Fatalf("unexpected error upserting project: %v", err)
	}

	svc := reqlog.NewService(reqlog.Config{
		Repository: db,
		Scope:      &scope.Scope{},
	})
	svc.SetActiveProjectID(projectID)

	next := func(req *http.Request) {
		req.Body = io.NopCloser(strings.NewReader("modified body"))
	}
	reqModFn := svc.RequestModifier(next)
	req := httptest.NewRequest("GET", "https://example.com/", strings.NewReader("bar"))
	reqID := ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy)
	req = req.WithContext(proxy.WithRequestID(req.Context(), reqID))

	reqModFn(req)

	// Request logs are stored via the repository's async batch writer;
	// flush to make sure the write has been committed.
	db.Flush()

	t.Run("request log was stored in repository", func(t *testing.T) {
		exp := reqlog.RequestLog{
			ID:        reqID,
			ProjectID: svc.ActiveProjectID(),
			Method:    req.Method,
			URL:       req.URL,
			Proto:     req.Proto,
			Header:    req.Header,
			Body:      []byte("modified body"),
		}

		got, err := svc.FindRequestLogByID(context.Background(), reqID)
		if err != nil {
			t.Fatalf("failed to find request by id: %v", err)
		}

		if diff := cmp.Diff(exp, got); diff != "" {
			t.Fatalf("request log not equal (-exp, +got):\n%v", diff)
		}
	})
}

//nolint:paralleltest
func TestResponseModifier(t *testing.T) {
	path := t.TempDir() + "bolt.db"
	boltDB, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("failed to open bolt database: %v", err)
	}
	defer boltDB.Close()

	db, err := bolt.DatabaseFromBoltDB(boltDB)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	projectID := ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy)
	err = db.UpsertProject(context.Background(), proj.Project{
		ID: projectID,
	})
	if err != nil {
		t.Fatalf("unexpected error upserting project: %v", err)
	}

	svc := reqlog.NewService(reqlog.Config{
		Repository: db,
	})
	svc.SetActiveProjectID(projectID)

	next := func(res *http.Response) error {
		res.Body = io.NopCloser(strings.NewReader("modified body"))
		return nil
	}
	resModFn := svc.ResponseModifier(next)

	req := httptest.NewRequest("GET", "https://example.com/", strings.NewReader("bar"))
	reqLogID := ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy)
	req = req.WithContext(context.WithValue(req.Context(), reqlog.ReqLogIDKey, reqLogID))

	err = db.StoreRequestLog(context.Background(), reqlog.RequestLog{
		ID:        reqLogID,
		ProjectID: projectID,
	})
	if err != nil {
		t.Fatalf("failed to store request log: %v", err)
	}

	res := &http.Response{
		Request: req,
		Body:    io.NopCloser(strings.NewReader("bar")),
	}

	if err := resModFn(res); err != nil {
		t.Fatalf("unexpected error (expected: nil, got: %v)", err)
	}

	// Response logs are stored via the repository's async batch writer;
	// flush to make sure the write has been committed.
	db.Flush()

	t.Run("request log was stored in repository", func(t *testing.T) {
		got, err := svc.FindRequestLogByID(context.Background(), reqLogID)
		if err != nil {
			t.Fatalf("failed to find request by id: %v", err)
		}

		t.Run("ran next modifier first, before calling repository", func(t *testing.T) {
			if exp := "modified body"; exp != string(got.Response.Body) {
				t.Fatalf("incorrect `ResponseLog.Body` value (expected: %v, got: %v)", exp, string(got.Response.Body))
			}
		})
	})
}

//nolint:paralleltest
func TestRequestModifierBodyLimit(t *testing.T) {
	path := t.TempDir() + "bolt.db"
	boltDB, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("failed to open bolt database: %v", err)
	}
	defer boltDB.Close()

	db, err := bolt.DatabaseFromBoltDB(boltDB)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	projectID := ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy)
	err = db.UpsertProject(context.Background(), proj.Project{
		ID: projectID,
	})
	if err != nil {
		t.Fatalf("unexpected error upserting project: %v", err)
	}

	svc := reqlog.NewService(reqlog.Config{
		Repository: db,
		Scope:      &scope.Scope{},
	})
	svc.SetActiveProjectID(projectID)

	fullBody := make([]byte, reqlog.MaxBodyLogSize+100)
	for i := range fullBody {
		fullBody[i] = 'a'
	}

	reqModFn := svc.RequestModifier(func(req *http.Request) {})
	req := httptest.NewRequest("POST", "https://example.com/", bytes.NewReader(fullBody))
	reqID := ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy)
	req = req.WithContext(proxy.WithRequestID(req.Context(), reqID))

	reqModFn(req)

	db.Flush()

	t.Run("stored request log body is truncated to the limit", func(t *testing.T) {
		got, err := svc.FindRequestLogByID(context.Background(), reqID)
		if err != nil {
			t.Fatalf("failed to find request by id: %v", err)
		}

		if exp := reqlog.MaxBodyLogSize; len(got.Body) != exp {
			t.Fatalf("incorrect stored body length (expected: %v, got: %v)", exp, len(got.Body))
		}
	})

	t.Run("downstream request body is left intact", func(t *testing.T) {
		got, err := io.ReadAll(req.Body)
		if err != nil {
			t.Fatalf("failed to read request body: %v", err)
		}

		if exp := len(fullBody); len(got) != exp {
			t.Fatalf("incorrect downstream body length (expected: %v, got: %v)", exp, len(got))
		}
	})
}

//nolint:paralleltest
func TestResponseModifierBodyLimit(t *testing.T) {
	path := t.TempDir() + "bolt.db"
	boltDB, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("failed to open bolt database: %v", err)
	}
	defer boltDB.Close()

	db, err := bolt.DatabaseFromBoltDB(boltDB)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	projectID := ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy)
	err = db.UpsertProject(context.Background(), proj.Project{
		ID: projectID,
	})
	if err != nil {
		t.Fatalf("unexpected error upserting project: %v", err)
	}

	svc := reqlog.NewService(reqlog.Config{
		Repository: db,
	})
	svc.SetActiveProjectID(projectID)

	reqLogID := ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy)
	err = db.StoreRequestLog(context.Background(), reqlog.RequestLog{
		ID:        reqLogID,
		ProjectID: projectID,
	})
	if err != nil {
		t.Fatalf("failed to store request log: %v", err)
	}

	fullBody := make([]byte, reqlog.MaxBodyLogSize+100)
	for i := range fullBody {
		fullBody[i] = 'b'
	}

	resModFn := svc.ResponseModifier(func(res *http.Response) error { return nil })

	req := httptest.NewRequest("GET", "https://example.com/", nil)
	req = req.WithContext(context.WithValue(req.Context(), reqlog.ReqLogIDKey, reqLogID))

	res := &http.Response{
		Request: req,
		Body:    io.NopCloser(bytes.NewReader(fullBody)),
	}

	if err := resModFn(res); err != nil {
		t.Fatalf("unexpected error (expected: nil, got: %v)", err)
	}

	db.Flush()

	t.Run("stored response log body is truncated to the limit", func(t *testing.T) {
		got, err := svc.FindRequestLogByID(context.Background(), reqLogID)
		if err != nil {
			t.Fatalf("failed to find request by id: %v", err)
		}

		if got.Response == nil {
			t.Fatal("expected response log to be stored, got: nil")
		}

		if exp := reqlog.MaxBodyLogSize; len(got.Response.Body) != exp {
			t.Fatalf("incorrect stored body length (expected: %v, got: %v)", exp, len(got.Response.Body))
		}
	})

	t.Run("downstream response body is left intact", func(t *testing.T) {
		got, err := io.ReadAll(res.Body)
		if err != nil {
			t.Fatalf("failed to read response body: %v", err)
		}

		if exp := len(fullBody); len(got) != exp {
			t.Fatalf("incorrect downstream body length (expected: %v, got: %v)", exp, len(got))
		}
	})
}
