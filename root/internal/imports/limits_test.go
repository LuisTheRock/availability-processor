package imports

import (
	"bytes"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"availability-processor/internal/availability"
	"availability-processor/internal/web"

	"go.uber.org/zap"
)

func submitCSV(t *testing.T, svc *Service, csv string) string {
	t.Helper()
	rows, parseErrs, err := parseCSV(strings.NewReader(csv))
	if err != nil {
		t.Fatal(err)
	}
	id, err := svc.Submit(t.Context(), rows, parseErrs)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// PDF §5: a stale row must stay stale even if a newer version lands while the job runs.
func TestNewerVersionArrivingWhileJobRuns(t *testing.T) {
	svc, d, versions, _ := newTestService(t)
	d.snapshotGate = make(chan struct{})

	id := submitCSV(t, svc, csvHeader+
		"s1,style-1,size-38,PT,10,7\n"+ // a newer event (v8) lands before this row is processed
		"s1,style-2,size-38,PT,5,1\n")

	// The job is parked waiting for the snapshot, so this is "while it runs".
	key := availability.Key{Seller: "s1", Product: "style-1", Size: "size-38", Country: "PT"}
	if _, err := versions.Record(t.Context(), key, 8); err != nil {
		t.Fatal(err)
	}
	close(d.snapshotGate)
	svc.Wait()

	sn, _, err := svc.Get(t.Context(), id, 10)
	if err != nil {
		t.Fatal(err)
	}
	if sn.Skipped != 1 || sn.Succeeded != 1 || sn.Failed != 0 {
		t.Errorf("skipped/succeeded/failed = %d/%d/%d, want 1/1/0", sn.Skipped, sn.Succeeded, sn.Failed)
	}
	for _, p := range d.putPaths() {
		if strings.Contains(p, "style-1") {
			t.Errorf("stale row wrote to the Availability API: %s", p)
		}
	}
}

// A failed snapshot fails only that country's rows, with no per-row lookups.
func TestSnapshotFailureFailsTheRows(t *testing.T) {
	svc, d, _, _ := newTestService(t)
	d.snapshotStatus = http.StatusInternalServerError // permanent: no retry delay in the test

	id := submitCSV(t, svc, csvHeader+"s1,style-1,size-38,PT,1,1\ns2,style-1,size-38,PT,1,1\n")
	svc.Wait()

	sn, errs, _ := svc.Get(t.Context(), id, 10)
	if sn.State != StateCompletedWithErrors || sn.Failed != 2 || len(errs) != 2 || errs[0].Code != "DEPENDENCY_FAILED" {
		t.Errorf("job = %+v errors = %+v", sn, errs)
	}
	if d.puts.Load() != 0 || d.lookups.Load() != 0 {
		t.Errorf("puts/lookups = %d/%d, want 0/0", d.puts.Load(), d.lookups.Load())
	}
}

// Two uploads at once share one limit of rowConcurrency rows per pod.
func TestRowLimitIsSharedAcrossJobs(t *testing.T) {
	svc, d, _, _ := newTestService(t)

	var cur atomic.Int64
	over := make(chan struct{}, 64)
	started := make(chan struct{}, 64)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseAll) // a failed check must not leave handlers blocked forever
	d.snapshotServed = make(chan struct{}, 4)
	d.putHook = func() {
		if cur.Add(1) > rowConcurrency {
			over <- struct{}{}
		}
		started <- struct{}{}
		<-release
		cur.Add(-1)
	}

	csv := func(prefix string) string {
		var b strings.Builder
		b.WriteString(csvHeader)
		for i := range 20 {
			fmt.Fprintf(&b, "%s%d,style-1,size-38,PT,1,1\n", prefix, i)
		}
		return b.String()
	}
	ids := []string{submitCSV(t, svc, csv("a")), submitCSV(t, svc, csv("b"))}

	for range rowConcurrency {
		<-started
	}
	<-d.snapshotServed
	<-d.snapshotServed // both jobs now have their context and are competing for slots

	// A negative check has to wait for something that must not happen.
	select {
	case <-over:
		t.Fatalf("more than %d rows in flight across two jobs", rowConcurrency)
	case <-time.After(300 * time.Millisecond):
	}

	releaseAll()
	svc.Wait()
	for _, id := range ids {
		if sn, _, _ := svc.Get(t.Context(), id, 1); sn.Succeeded != 20 {
			t.Errorf("job %s succeeded = %d, want 20", id, sn.Succeeded)
		}
	}
}

// failed is the true count; the stored/listed errors are a capped sample.
func TestErrorListIsCapped(t *testing.T) {
	svc, _, _, _ := newTestService(t)

	var b strings.Builder
	b.WriteString(csvHeader)
	for i := range maxErrorsLimit + 500 {
		fmt.Fprintf(&b, "s%d,style-missing,size-38,PT,1,1\n", i)
	}
	id := submitCSV(t, svc, b.String())
	svc.Wait()

	sn, errs, _ := svc.Get(t.Context(), id, 100000)
	if sn.Failed != maxErrorsLimit+500 || len(errs) != maxErrorsLimit {
		t.Errorf("failed = %d (want %d), listed = %d (want %d)", sn.Failed, maxErrorsLimit+500, len(errs), maxErrorsLimit)
	}
}

func TestUploadLimits(t *testing.T) {
	svc, _, _, _ := newTestService(t)
	h := NewHandler(svc, zap.NewNop())

	bigCSV := csvHeader + "s1," + strings.Repeat("a", maxUploadBytes+1<<20) + ",size-38,PT,1,1\n"
	multipartBody := func(csv string) (*bytes.Buffer, string) {
		var buf bytes.Buffer
		mw := multipart.NewWriter(&buf)
		fw, _ := mw.CreateFormFile("file", "stock.csv")
		_, _ = fw.Write([]byte(csv))
		_ = mw.Close()
		return &buf, mw.FormDataContentType()
	}

	tests := []struct {
		name string
		body func() (*bytes.Buffer, string)
		want int
	}{
		{"raw upload over the cap", func() (*bytes.Buffer, string) { return bytes.NewBufferString(bigCSV), "text/csv" }, http.StatusRequestEntityTooLarge},
		{"multipart upload over the cap", func() (*bytes.Buffer, string) { return multipartBody(bigCSV) }, http.StatusRequestEntityTooLarge},
		{"valid multipart upload", func() (*bytes.Buffer, string) {
			return multipartBody(csvHeader + "s1,style-1,size-38,PT,1,1\n")
		}, http.StatusAccepted},
		{"multipart without a file field", func() (*bytes.Buffer, string) {
			var buf bytes.Buffer
			mw := multipart.NewWriter(&buf)
			_ = mw.WriteField("other", "x")
			_ = mw.Close()
			return &buf, mw.FormDataContentType()
		}, http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, contentType := tt.body()
			req := httptest.NewRequest(http.MethodPost, "/v1/availability/imports", body)
			req.Header.Set("Content-Type", contentType)

			rec := httptest.NewRecorder()
			web.Make(h.HandleCreate)(rec, req)
			svc.Wait()

			if rec.Code != tt.want {
				t.Errorf("status = %d (%.80s), want %d", rec.Code, rec.Body, tt.want)
			}
		})
	}
}
