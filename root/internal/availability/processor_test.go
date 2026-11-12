package availability

import (
	"availability-processor/internal/web"
	"context"
	"encoding/json"
	"errors"
	"go.uber.org/zap"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"availability-processor/internal/upstream"
)

// fakeDeps stands in for the Product Context API, Availability API and event sink.
type fakeDeps struct {
	mu          sync.Mutex
	lookups     int
	puts        int
	events      int
	lastPut     map[string]any
	lastEvent   map[string]any
	failPublish bool
}

func (f *fakeDeps) handler(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	switch {
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/products/"):
		f.lookups++
		parts := strings.Split(r.URL.Path, "/") // "", v1, products, {id}, sizes, {size}
		pc := upstream.ProductContext{
			ProductID: parts[3], ProductName: "Linen Shirt", Brand: upstream.Named{ID: "brand-42", Name: "Atelier"},
			ProductStatus: "ACTIVE", SizeID: parts[5], SizeLabel: "EU 38", SizeSystem: "EU",
			CountryCode: r.URL.Query().Get("countryCode"), CountryEnabled: true,
		}
		switch parts[3] {
		case "style-429":
			w.Header().Set("Retry-After", "5")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		case "style-missing":
			http.NotFound(w, r)
			return
		case "style-inactive":
			pc.ProductStatus = "INACTIVE"
		case "style-disabled":
			pc.CountryEnabled = false
		}
		_ = json.NewEncoder(w).Encode(pc)
	case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/availability/"):
		f.puts++
		f.lastPut = decode(r.Body)
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodPost && r.URL.Path == "/mock-broker/v1/events":
		f.events++
		f.lastEvent = decode(r.Body)
		if f.failPublish {
			w.WriteHeader(http.StatusBadRequest) // permanent: no retry delay in the test
			return
		}
		w.WriteHeader(http.StatusAccepted)
	default:
		http.NotFound(w, r)
	}
}

func decode(r io.Reader) map[string]any {
	var m map[string]any
	_ = json.NewDecoder(r).Decode(&m)
	return m
}

func newProcessor(t *testing.T) (*Processor, *fakeDeps, *MemoryVersions) {
	t.Helper()
	deps := &fakeDeps{}
	srv := httptest.NewServer(http.HandlerFunc(deps.handler))
	t.Cleanup(srv.Close)
	versions := &MemoryVersions{}
	return NewProcessor(upstream.New(srv.URL), versions), deps, versions
}

func change(product string, version int64, qty int) Change {
	return Change{
		SellerID: "boutique-42", ProductID: product, SizeID: "size-38", CountryCode: "PT",
		AvailableQuantity: qty, SourceVersion: version,
	}
}

func TestProcessEvent(t *testing.T) {
	tests := []struct {
		name       string
		prior      []Change // applied first, through the same processor
		change     Change
		failPub    bool
		wantStatus string
		wantReason string
		wantErr    error
		wantCalls  [3]int // lookups, puts, events, counted after the prior changes
	}{
		{name: "applied", change: change("style-123", 7, 3), wantStatus: StatusApplied, wantCalls: [3]int{1, 1, 1}},
		{
			name: "duplicate version with a new eventId is skipped", prior: []Change{change("style-123", 7, 3)},
			change: Change{EventID: "msg-replay", SellerID: "boutique-42", ProductID: "style-123", SizeID: "size-38",
				CountryCode: "PT", AvailableQuantity: 3, SourceVersion: 7},
			wantStatus: StatusSkipped, wantReason: ReasonDuplicate, wantCalls: [3]int{1, 1, 1},
		},
		{
			name: "stale version leaves stock untouched", prior: []Change{change("style-123", 8, 3)},
			change: change("style-123", 7, 10), wantStatus: StatusSkipped, wantReason: ReasonStale,
			wantCalls: [3]int{1, 1, 1},
		},
		{
			name: "version of another seller is independent", prior: []Change{change("style-123", 8, 3)},
			change:     Change{SellerID: "other", ProductID: "style-123", SizeID: "size-38", CountryCode: "PT", AvailableQuantity: 1, SourceVersion: 1},
			wantStatus: StatusApplied, wantCalls: [3]int{2, 2, 2},
		},
		{name: "unknown product", change: change("style-missing", 1, 1), wantErr: ErrUnknownProduct, wantCalls: [3]int{1, 0, 0}},
		{name: "inactive product", change: change("style-inactive", 1, 1), wantErr: ErrInactiveProduct, wantCalls: [3]int{1, 0, 0}},
		{name: "country not enabled", change: change("style-disabled", 1, 1), wantErr: ErrCountryDisabled, wantCalls: [3]int{1, 0, 0}},
		{name: "negative quantity", change: change("style-123", 1, -1), wantErr: ErrInvalid},
		{name: "bad country", change: Change{SellerID: "s", ProductID: "p", SizeID: "z", CountryCode: "pt"}, wantErr: ErrInvalid},
		{name: "publish failure after the write", change: change("style-123", 7, 3), failPub: true, wantErr: ErrPublish, wantCalls: [3]int{1, 1, 1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, deps, _ := newProcessor(t)
			for _, c := range tt.prior {
				if _, err := p.ProcessEvent(t.Context(), c); err != nil {
					t.Fatalf("prior change: %v", err)
				}
			}
			// calls made by prior changes are part of wantCalls
			deps.failPublish = tt.failPub

			got, err := p.ProcessEvent(t.Context(), tt.change)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if got.Status != tt.wantStatus || got.Reason != tt.wantReason {
				t.Errorf("result = %+v, want %s/%s", got, tt.wantStatus, tt.wantReason)
			}
			if tt.wantCalls != ([3]int{}) {
				if calls := [3]int{deps.lookups, deps.puts, deps.events}; calls != tt.wantCalls {
					t.Errorf("lookups/puts/events = %v, want %v", calls, tt.wantCalls)
				}
			}
		})
	}
}

func TestProcessEventBodies(t *testing.T) {
	p, deps, versions := newProcessor(t)

	if _, err := p.ProcessEvent(t.Context(), change("style-123", 7, 3)); err != nil {
		t.Fatal(err)
	}

	if deps.lastPut["availableQuantity"] != float64(3) || deps.lastPut["sourceVersion"] != float64(7) {
		t.Errorf("PUT body = %v", deps.lastPut)
	}
	if deps.lastPut["productContext"].(map[string]any)["sizeLabel"] != "EU 38" {
		t.Errorf("PUT body lacks product context: %v", deps.lastPut)
	}
	ev := deps.lastEvent
	if ev["eventType"] != "SizeAvailabilityUpdated" || ev["sellerId"] != "boutique-42" || ev["sourceVersion"] != float64(7) {
		t.Errorf("event = %v", ev)
	}
	if v, _, _ := versions.Applied(t.Context(), change("style-123", 0, 0).Key()); v != 7 {
		t.Errorf("recorded version = %d, want 7", v)
	}
}

// A failed publish must not be re-applied by a replay of the same version.
func TestPublishFailureIsNotReapplied(t *testing.T) {
	p, deps, _ := newProcessor(t)
	deps.failPublish = true

	if _, err := p.ProcessEvent(t.Context(), change("style-123", 7, 3)); !errors.Is(err, ErrPublish) {
		t.Fatalf("err = %v, want ErrPublish", err)
	}
	got, err := p.ProcessEvent(t.Context(), change("style-123", 7, 3))
	if err != nil || got.Reason != ReasonDuplicate {
		t.Fatalf("replay = %+v, %v; want skipped DUPLICATE", got, err)
	}
	if deps.puts != 1 {
		t.Errorf("puts = %d, want 1", deps.puts)
	}
}

// A throttled dependency must reach the event caller as 503 + Retry-After
// after a single attempt, with the key lock released.
func TestEventRateLimitedFailsFast(t *testing.T) {
	p, deps, _ := newProcessor(t)
	h := NewHandler(p, zap.NewNop())

	body := `{"eventType":"SellerInventoryChanged","schemaVersion":1,"sellerId":"s","productId":"style-429","sizeId":"size-38","countryCode":"PT","availableQuantity":1,"sourceVersion":1}`
	rec := httptest.NewRecorder()
	start := time.Now()
	web.Make(h.HandleMessage)(rec, httptest.NewRequest(http.MethodPost, "/_dev/messages", strings.NewReader(body)))

	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") != "5" {
		t.Errorf("status = %d, Retry-After = %q; want 503 and 5", rec.Code, rec.Header().Get("Retry-After"))
	}
	if deps.lookups != 1 || time.Since(start) > time.Second {
		t.Errorf("lookups = %d in %v; want 1 attempt and no sleeping", deps.lookups, time.Since(start))
	}
	if _, err := p.ProcessEvent(t.Context(), change("style-123", 1, 1)); err != nil { // same stock key family is not blocked
		t.Errorf("lock not released or processor stuck: %v", err)
	}
}

func TestOutputEventID(t *testing.T) {
	p, deps, _ := newProcessor(t)

	withID := change("style-123", 1, 1)
	withID.EventID = "fault-503"
	if _, err := p.ProcessEvent(t.Context(), withID); err != nil {
		t.Fatal(err)
	}
	if deps.lastEvent["eventId"] != "fault-503" {
		t.Errorf("event with an inbound eventId published %v, want it reused", deps.lastEvent["eventId"])
	}

	csvRow := change("style-123", 2, 1) // no eventId, like a CSV row
	if _, err := p.ProcessEvent(t.Context(), csvRow); err != nil {
		t.Fatal(err)
	}
	if id, _ := deps.lastEvent["eventId"].(string); !strings.HasPrefix(id, "out-") {
		t.Errorf("row without eventId published %q, want an out-… id", id)
	}
}

// lostRace reports that a newer version was recorded while we were writing.
type lostRace struct{ *MemoryVersions }

func (lostRace) Record(context.Context, Key, int64) (bool, error) { return false, nil }

func TestLostVersionRaceDoesNotPublish(t *testing.T) {
	p, deps, _ := newProcessor(t)
	p.versions = lostRace{&MemoryVersions{}}

	got, err := p.ProcessEvent(t.Context(), change("style-123", 7, 3))
	if err != nil || got.Reason != ReasonStale {
		t.Fatalf("got %+v, %v; want skipped STALE", got, err)
	}
	if deps.events != 0 {
		t.Errorf("events = %d, want 0: a row that lost the race must not announce itself", deps.events)
	}
}

// One row must be bounded below the lock lease so the lock cannot expire under it.
func TestRowIsBoundedByTheLockLease(t *testing.T) {
	p, _, _ := newProcessor(t)

	var left time.Duration
	_, _ = p.Process(t.Context(), change("style-123", 1, 1), func(ctx context.Context, _ Change) (upstream.ProductContext, error) {
		d, ok := ctx.Deadline()
		if !ok {
			t.Error("no deadline on the row context")
		}
		left = time.Until(d)
		return upstream.ProductContext{}, upstream.ErrNotFound
	})
	if left <= 0 || left >= lockLease {
		t.Errorf("row deadline in %v, want 0 < d < lock lease (%v)", left, lockLease)
	}
}

func TestEventValidationAtTheBoundary(t *testing.T) {
	base := `"eventType":"SellerInventoryChanged","schemaVersion":1,"sellerId":"s","productId":"style-123","sizeId":"size-38","countryCode":"PT"`
	tests := []struct {
		name string
		body string
		want int
	}{
		{"valid", `{` + base + `,"availableQuantity":3,"sourceVersion":7}`, http.StatusOK},
		{"missing quantity must not become zero stock", `{` + base + `,"sourceVersion":7}`, http.StatusBadRequest},
		{"missing version", `{` + base + `,"availableQuantity":3}`, http.StatusBadRequest},
		{"misspelled quantity", `{` + base + `,"availableQuantty":3,"sourceVersion":7}`, http.StatusBadRequest},
		{"explicit zero quantity is valid", `{` + base + `,"availableQuantity":0,"sourceVersion":7}`, http.StatusOK},
		{"wrong event type", `{"eventType":"Other","schemaVersion":1,"sellerId":"s","productId":"p","sizeId":"z","countryCode":"PT","availableQuantity":1,"sourceVersion":1}`, http.StatusBadRequest},
		{"not json", `nope`, http.StatusBadRequest},
		{"negative quantity", `{` + base + `,"availableQuantity":-1,"sourceVersion":7}`, http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, deps, _ := newProcessor(t)
			h := NewHandler(p, zap.NewNop())

			rec := httptest.NewRecorder()
			web.Make(h.HandleMessage)(rec, httptest.NewRequest(http.MethodPost, "/_dev/messages", strings.NewReader(tt.body)))

			if rec.Code != tt.want {
				t.Errorf("status = %d (%s), want %d", rec.Code, rec.Body, tt.want)
			}
			if tt.want == http.StatusBadRequest && deps.puts != 0 {
				t.Errorf("a rejected event wrote to the Availability API")
			}
		})
	}
}

// With several fields missing, the error always names the first one in a fixed order.
func TestValidateNamesTheFirstMissingField(t *testing.T) {
	for range 20 { // random map order would show up within a few runs
		err := Change{CountryCode: "PT"}.Validate() // seller, product and size all missing
		if !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), "sellerId") {
			t.Fatalf("err = %v, want it to name sellerId first", err)
		}
	}
}
