package imports

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"availability-processor/internal/availability"
	"availability-processor/internal/upstream"
	"availability-processor/internal/web"

	"go.uber.org/zap"
)

// mapStore is a fake jobStore with the same append-only error semantics as Redis.
type mapStore struct {
	mu    sync.Mutex
	snaps map[string]Snapshot
	errs  map[string][]RowError
}

func newMapStore() *mapStore {
	return &mapStore{snaps: map[string]Snapshot{}, errs: map[string][]RowError{}}
}

func (m *mapStore) Save(_ context.Context, s Snapshot, newErrs []RowError) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.snaps[s.ID] = s
	m.errs[s.ID] = append(m.errs[s.ID], newErrs...)
	return nil
}

func (m *mapStore) Load(_ context.Context, id string, limit int) (Snapshot, []RowError, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.snaps[id]
	if !ok {
		return Snapshot{}, nil, ErrJobNotFound
	}
	e := m.errs[id]
	return s, e[:min(limit, len(e))], nil
}

type deps struct {
	snapshots, lookups, puts, events atomic.Int64

	// Test hooks: set before the first request.
	snapshotGate   chan struct{} // the snapshot waits until it is closed
	snapshotServed chan struct{} // receives one value per snapshot served
	snapshotStatus int           // forces the snapshot's HTTP status when non-zero
	putHook        func()        // runs on every PUT before it answers; may block

	mu      sync.Mutex
	putURLs []string
}

func (d *deps) putPaths() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.putURLs...)
}

func newDeps(t *testing.T) (*deps, string) {
	t.Helper()
	d := &deps{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/product-contexts/snapshot":
			d.snapshots.Add(1)
			if d.snapshotGate != nil {
				<-d.snapshotGate
			}
			if d.snapshotStatus != 0 {
				w.WriteHeader(d.snapshotStatus)
				return
			}
			if d.snapshotServed != nil {
				defer func() { d.snapshotServed <- struct{}{} }()
			}
			ctxs := []upstream.ProductContext{}
			for _, id := range []string{"style-1", "style-2", "style-inactive"} {
				status := "ACTIVE"
				if id == "style-inactive" {
					status = "INACTIVE"
				}
				ctxs = append(ctxs, upstream.ProductContext{
					ProductID: id, ProductStatus: status, SizeID: "size-38", CountryCode: "PT", CountryEnabled: true,
				})
			}
			_ = json.NewEncoder(w).Encode(ctxs)
		case strings.HasPrefix(r.URL.Path, "/v1/products/"):
			d.lookups.Add(1)
			http.NotFound(w, r)
		case r.Method == http.MethodPut:
			d.puts.Add(1)
			d.mu.Lock()
			d.putURLs = append(d.putURLs, r.URL.Path)
			d.mu.Unlock()
			if d.putHook != nil {
				d.putHook()
			}
		case r.Method == http.MethodPost:
			d.events.Add(1)
			w.WriteHeader(http.StatusAccepted)
		}
	}))
	t.Cleanup(srv.Close)
	return d, srv.URL
}

func newTestService(t *testing.T) (*Service, *deps, *availability.MemoryVersions, *mapStore) {
	t.Helper()
	d, url := newDeps(t)
	client := upstream.New(url)
	versions := &availability.MemoryVersions{}
	store := newMapStore()
	return newService(availability.NewProcessor(client, versions), client, store, zap.NewNop()), d, versions, store
}

const csvHeader = "sellerId,productId,sizeId,countryCode,availableQuantity,sourceVersion\n"

func TestImportPartialFailures(t *testing.T) {
	svc, d, versions, _ := newTestService(t)
	// style-2 is already at version 8, so the CSV's version 7 is stale.
	if _, err := versions.Record(t.Context(), availability.Key{Seller: "s1", Product: "style-2", Size: "size-38", Country: "PT"}, 8); err != nil {
		t.Fatal(err)
	}

	rows, parseErrs, err := parseCSV(strings.NewReader(csvHeader +
		"s1,style-1,size-38,PT,3,7\n" + // applied
		"s1,style-2,size-38,PT,10,7\n" + // stale -> skipped
		"s1,style-missing,size-38,PT,1,1\n" + // not in snapshot -> failed
		"s1,style-inactive,size-38,PT,1,1\n" + // inactive -> failed
		"s1,style-1,size-38,PT,abc,1\n")) // malformed row -> failed, others continue
	if err != nil {
		t.Fatal(err)
	}

	id, err := svc.Submit(t.Context(), rows, parseErrs)
	if err != nil {
		t.Fatal(err)
	}
	svc.Wait()

	sn, errs, err := svc.Get(t.Context(), id, 100)
	if err != nil {
		t.Fatal(err)
	}

	want := Snapshot{Total: 5, Processed: 5, Succeeded: 1, Skipped: 1, Failed: 3, State: StateCompletedWithErrors}
	got := Snapshot{Total: sn.Total, Processed: sn.Processed, Succeeded: sn.Succeeded, Skipped: sn.Skipped, Failed: sn.Failed, State: sn.State}
	if got != want {
		t.Errorf("snapshot = %+v, want %+v", got, want)
	}

	codes := map[string]int{}
	for _, e := range errs {
		codes[e.Code]++
	}
	if len(errs) != 3 || codes["UNKNOWN_PRODUCT"] != 1 || codes["INACTIVE_PRODUCT"] != 1 || codes["INVALID"] != 1 {
		t.Errorf("row errors = %+v", errs)
	}

	if d.snapshots.Load() != 1 || d.lookups.Load() != 0 {
		t.Errorf("snapshot GETs = %d (want 1), single lookups = %d (want 0)", d.snapshots.Load(), d.lookups.Load())
	}
	if d.puts.Load() != 1 || d.events.Load() != 1 {
		t.Errorf("puts/events = %d/%d, want 1/1 (stale row must not write or publish)", d.puts.Load(), d.events.Load())
	}
}

func TestOneSnapshotPerCountry(t *testing.T) {
	svc, d, _, _ := newTestService(t)
	rows, parseErrs, err := parseCSV(strings.NewReader(csvHeader +
		"s1,style-1,size-38,PT,1,1\ns2,style-1,size-38,PT,1,1\ns1,style-2,size-38,PT,1,1\n"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Submit(t.Context(), rows, parseErrs); err != nil {
		t.Fatal(err)
	}
	svc.Wait()

	if d.snapshots.Load() != 1 {
		t.Errorf("snapshot GETs = %d, want 1 for 3 rows in one country", d.snapshots.Load())
	}
}

func TestHandlers(t *testing.T) {
	svc, _, _, store := newTestService(t)
	h := NewHandler(svc, zap.NewNop())

	t.Run("missing column is a synchronous 400", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/v1/availability/imports", strings.NewReader("sellerId,productId\ns1,p1\n"))
		web.Make(h.HandleCreate)(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want 400", rec.Code)
		}
	})

	t.Run("empty body is 400", func(t *testing.T) {
		rec := httptest.NewRecorder()
		web.Make(h.HandleCreate)(rec, httptest.NewRequest(http.MethodPost, "/v1/availability/imports", strings.NewReader("")))
		if rec.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want 400", rec.Code)
		}
	})

	t.Run("valid upload is 202 with a job id", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/v1/availability/imports", strings.NewReader(csvHeader+"s1,style-1,size-38,PT,1,1\n"))
		web.Make(h.HandleCreate)(rec, req)
		svc.Wait()

		var body map[string]string
		_ = json.NewDecoder(rec.Body).Decode(&body)
		if rec.Code != http.StatusAccepted || body["jobId"] == "" {
			t.Errorf("status = %d body = %v, want 202 with jobId", rec.Code, body)
		}
	})

	t.Run("status of a job that only exists in the store", func(t *testing.T) {
		_ = store.Save(t.Context(), Snapshot{ID: "from-another-pod", State: StateCompleted, Total: 2, Processed: 2, Succeeded: 2}, nil)

		mux := http.NewServeMux()
		mux.HandleFunc("GET /{jobId}", web.Make(h.HandleGet))

		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/from-another-pod", nil))
		if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"succeeded":2`) {
			t.Errorf("status = %d body = %s", rec.Code, rec.Body)
		}

		rec = httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/nope", nil))
		if rec.Code != http.StatusNotFound {
			t.Errorf("unknown job status = %d, want 404", rec.Code)
		}
	})
}
