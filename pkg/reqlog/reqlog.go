package reqlog

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/oklog/ulid"

	"github.com/dstotijn/hetty/pkg/filter"
	"github.com/dstotijn/hetty/pkg/log"
	"github.com/dstotijn/hetty/pkg/proxy"
	"github.com/dstotijn/hetty/pkg/scope"
)

type contextKey int

const (
	LogBypassedKey contextKey = iota
	ReqLogIDKey
)

// DefaultMaxBodySize is the default cap (10 MiB) on how many bytes of a
// request/response body are read into memory for logging.
const DefaultMaxBodySize int64 = 10 << 20

var (
	ErrRequestNotFound    = errors.New("reqlog: request not found")
	ErrProjectIDMustBeSet = errors.New("reqlog: project ID must be set")
)

type RequestLog struct {
	ID        ulid.ULID
	ProjectID ulid.ULID

	URL    *url.URL
	Method string
	Proto  string
	Header http.Header
	Body   []byte

	Response *ResponseLog
}

type ResponseLog struct {
	Proto      string
	StatusCode int
	Status     string
	Header     http.Header
	Body       []byte
}

type Service struct {
	bypassOutOfScopeRequests bool
	findReqsFilter           FindRequestsFilter
	activeProjectID          ulid.ULID
	scope                    *scope.Scope
	repo                     Repository
	logger                   log.Logger
	maxBodySize              int64
}

type FindRequestsFilter struct {
	ProjectID   ulid.ULID
	OnlyInScope bool
	SearchExpr  filter.Expression
}

type Config struct {
	ActiveProjectID ulid.ULID
	Scope           *scope.Scope
	Repository      Repository
	Logger          log.Logger
	// MaxBodySize caps the number of body bytes buffered in memory for
	// logging. A zero value uses DefaultMaxBodySize; a negative value
	// removes the limit.
	MaxBodySize int64
}

func NewService(cfg Config) *Service {
	maxBodySize := cfg.MaxBodySize
	if maxBodySize == 0 {
		maxBodySize = DefaultMaxBodySize
	}

	s := &Service{
		activeProjectID: cfg.ActiveProjectID,
		repo:            cfg.Repository,
		scope:           cfg.Scope,
		logger:          cfg.Logger,
		maxBodySize:     maxBodySize,
	}

	if s.logger == nil {
		s.logger = log.NewNopLogger()
	}

	return s
}

func (svc *Service) FindRequests(ctx context.Context) ([]RequestLog, error) {
	return svc.repo.FindRequestLogs(ctx, svc.findReqsFilter, svc.scope)
}

func (svc *Service) FindRequestLogByID(ctx context.Context, id ulid.ULID) (RequestLog, error) {
	return svc.repo.FindRequestLogByID(ctx, svc.activeProjectID, id)
}

func (svc *Service) ClearRequests(ctx context.Context, projectID ulid.ULID) error {
	return svc.repo.ClearRequestLogs(ctx, projectID)
}

func (svc *Service) storeResponse(ctx context.Context, reqLogID ulid.ULID, res *http.Response) error {
	resLog, err := ParseHTTPResponse(res)
	if err != nil {
		return err
	}

	return svc.repo.StoreResponseLog(ctx, svc.activeProjectID, reqLogID, resLog)
}

// readBodyForLog reads at most maxBodySize bytes from r for logging, while
// returning a replacement reader that still yields the complete, original
// body so it can be forwarded upstream/to the client. This prevents an
// oversized body from exhausting memory for logging purposes.
//
// It returns the (possibly truncated) body captured for logging, whether the
// body exceeded the cap, and a reader for the full original body. When
// maxBodySize is negative, the whole body is read (no protection).
func readBodyForLog(r io.Reader, maxBodySize int64) (logged []byte, truncated bool, full io.Reader, err error) {
	if maxBodySize < 0 {
		logged, err = io.ReadAll(r)
		if err != nil {
			return nil, false, nil, err
		}

		return logged, false, bytes.NewReader(logged), nil
	}

	// Read one byte beyond the cap (using io.LimitReader) so we can both
	// bound memory usage and detect whether the body was truncated.
	read, err := io.ReadAll(io.LimitReader(r, maxBodySize+1))
	if err != nil {
		return nil, false, nil, err
	}

	if int64(len(read)) <= maxBodySize {
		// Body fit entirely within the cap; it is safe to forward verbatim.
		return read, false, bytes.NewReader(read), nil
	}

	// Body exceeded the cap: log only the first maxBodySize bytes, but
	// reconstruct the complete body for forwarding by concatenating the
	// captured prefix with the unread remainder of the source reader.
	capped := make([]byte, maxBodySize)
	copy(capped, read)
	full = io.MultiReader(bytes.NewReader(read), r)

	return capped, true, full, nil
}

func (svc *Service) RequestModifier(next proxy.RequestModifyFunc) proxy.RequestModifyFunc {
	return func(req *http.Request) {
		next(req)

		clone := req.Clone(req.Context())

		var body []byte

		if req.Body != nil {
			originalBody := req.Body
			loggedBody, truncated, fullBody, err := readBodyForLog(originalBody, svc.maxBodySize)
			if err != nil {
				svc.logger.Errorw("Failed to read request body for logging.",
					"error", err)
				return
			}
			// When the body fit within the cap it was fully consumed here;
			// close it to release the underlying connection. When it was
			// truncated, the original reader is still needed to supply the
			// remainder to the forwarded request and must stay open.
			if !truncated {
				originalBody.Close()
			}

			body = loggedBody

			// Restore the complete body for the forwarded request; the clone
			// (used for logging and scope matching) only carries the capped
			// body.
			req.Body = io.NopCloser(fullBody)
			clone.Body = io.NopCloser(bytes.NewReader(loggedBody))

			if truncated {
				svc.logger.Debugw("Request body exceeded log size cap; truncating logged body.",
					"url", req.URL.String(), "maxBodySize", svc.maxBodySize)
			}
		}

		// Bypass logging if no project is active.
		if svc.activeProjectID.Compare(ulid.ULID{}) == 0 {
			ctx := context.WithValue(req.Context(), LogBypassedKey, true)
			*req = *req.WithContext(ctx)

			svc.logger.Debugw("Bypassed logging: no active project.",
				"url", req.URL.String())

			return
		}

		// Bypass logging if this setting is enabled and the incoming request
		// doesn't match any scope rules.
		if svc.bypassOutOfScopeRequests && !svc.scope.Match(clone, body) {
			ctx := context.WithValue(req.Context(), LogBypassedKey, true)
			*req = *req.WithContext(ctx)

			svc.logger.Debugw("Bypassed logging: request doesn't match any scope rules.",
				"url", req.URL.String())

			return
		}

		reqID, ok := proxy.RequestIDFromContext(req.Context())
		if !ok {
			svc.logger.Errorw("Bypassed logging: request doesn't have an ID.")
			return
		}

		reqLog := RequestLog{
			ID:        reqID,
			ProjectID: svc.activeProjectID,
			Method:    clone.Method,
			URL:       clone.URL,
			Proto:     clone.Proto,
			Header:    clone.Header,
			Body:      body,
		}

		err := svc.repo.StoreRequestLog(req.Context(), reqLog)
		if err != nil {
			svc.logger.Errorw("Failed to store request log.",
				"error", err)
			return
		}

		svc.logger.Debugw("Stored request log.",
			"reqLogID", reqLog.ID.String(),
			"url", reqLog.URL.String())

		ctx := context.WithValue(req.Context(), ReqLogIDKey, reqLog.ID)
		*req = *req.WithContext(ctx)
	}
}

func (svc *Service) ResponseModifier(next proxy.ResponseModifyFunc) proxy.ResponseModifyFunc {
	return func(res *http.Response) error {
		if err := next(res); err != nil {
			return err
		}

		if bypassed, _ := res.Request.Context().Value(LogBypassedKey).(bool); bypassed {
			return nil
		}

		reqLogID, ok := res.Request.Context().Value(ReqLogIDKey).(ulid.ULID)
		if !ok {
			return errors.New("reqlog: request is missing ID")
		}

		clone := *res

		if res.Body != nil {
			originalBody := res.Body
			loggedBody, truncated, fullBody, err := readBodyForLog(originalBody, svc.maxBodySize)
			if err != nil {
				return fmt.Errorf("reqlog: could not read response body: %w", err)
			}
			// When the body fit within the cap it was fully consumed here;
			// close it to release the underlying connection. When it was
			// truncated, the original reader still supplies the remainder to
			// the client and must stay open.
			if !truncated {
				originalBody.Close()
			}

			// The client receives the complete body; only the stored log is
			// capped.
			res.Body = io.NopCloser(fullBody)
			clone.Body = io.NopCloser(bytes.NewReader(loggedBody))

			if truncated {
				svc.logger.Debugw("Response body exceeded log size cap; truncating logged body.",
					"reqLogID", reqLogID.String(), "maxBodySize", svc.maxBodySize)
			}
		}

		go func() {
			if err := svc.storeResponse(context.Background(), reqLogID, &clone); err != nil {
				svc.logger.Errorw("Failed to store response log.",
					"error", err)
			} else {
				svc.logger.Debugw("Stored response log.",
					"reqLogID", reqLogID.String())
			}
		}()

		return nil
	}
}

func (svc *Service) SetActiveProjectID(id ulid.ULID) {
	svc.activeProjectID = id
}

func (svc *Service) ActiveProjectID() ulid.ULID {
	return svc.activeProjectID
}

func (svc *Service) SetFindReqsFilter(filter FindRequestsFilter) {
	svc.findReqsFilter = filter
}

func (svc *Service) FindReqsFilter() FindRequestsFilter {
	return svc.findReqsFilter
}

func (svc *Service) SetBypassOutOfScopeRequests(bypass bool) {
	svc.bypassOutOfScopeRequests = bypass
}

func (svc *Service) BypassOutOfScopeRequests() bool {
	return svc.bypassOutOfScopeRequests
}

func ParseHTTPResponse(res *http.Response) (ResponseLog, error) {
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return ResponseLog{}, fmt.Errorf("reqlog: could not read body: %w", err)
	}

	return ResponseLog{
		Proto:      res.Proto,
		StatusCode: res.StatusCode,
		Status:     res.Status,
		Header:     res.Header,
		Body:       body,
	}, nil
}
