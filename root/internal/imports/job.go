// Package imports owns CSV bulk imports: parsing, job lifecycle, the async
// worker and the job HTTP handlers.
package imports

import (
	"context"
	"errors"
	"sync"
	"time"

	"availability-processor/internal/availability"
)

const (
	StatePending             = "PENDING"
	StateRunning             = "RUNNING"
	StateCompleted           = "COMPLETED"
	StateCompletedWithErrors = "COMPLETED_WITH_ERRORS"
	StateInterrupted         = "INTERRUPTED"

	staleAfter = 30 * time.Second
)

var ErrJobNotFound = errors.New("job not found")

type RowError struct {
	Row     int    `json:"row"` // 1-based data row, header excluded
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Snapshot is the counter view of a job. Skipped rows (stale/duplicate) are
// counted but not listed; Failed is the true count, while the listed RowErrors
// are capped at maxErrorsLimit.
type Snapshot struct {
	ID        string    `json:"jobId"`
	State     string    `json:"state"`
	Total     int64     `json:"total"`
	Processed int64     `json:"processed"`
	Succeeded int64     `json:"succeeded"`
	Failed    int64     `json:"failed"`
	Skipped   int64     `json:"skipped"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// jobStore is the durable copy of job status (Redis in production).
type jobStore interface {
	// Save writes the counters and appends only the new row errors.
	Save(ctx context.Context, s Snapshot, newErrs []RowError) error
	// Load returns ErrJobNotFound when the id is unknown.
	Load(ctx context.Context, id string, errorsLimit int) (Snapshot, []RowError, error)
}

// Job is the live in-memory working copy of a running import.
type Job struct {
	mu      sync.Mutex
	snap    Snapshot
	errs    []RowError
	flushed int // errs[:flushed] are already in the store
}

func newJob(id string, total int) *Job {
	now := time.Now().UTC()
	return &Job{snap: Snapshot{ID: id, State: StatePending, Total: int64(total), CreatedAt: now, UpdatedAt: now}}
}

func (j *Job) setState(state string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.snap.State = state
}

func (j *Job) fail(row int, code, msg string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.snap.Processed++
	j.snap.Failed++
	if len(j.errs) < maxErrorsLimit { // failed keeps the true count; the list is a sample
		j.errs = append(j.errs, RowError{row, code, msg})
	}
}

func (j *Job) record(row int, res availability.Result, err error) {
	if err != nil {
		j.fail(row, availability.Code(err), err.Error())
		return
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	j.snap.Processed++
	if res.Status == availability.StatusSkipped {
		j.snap.Skipped++
		return
	}
	j.snap.Succeeded++
}

// finish sets the terminal state from the counters.
func (j *Job) finish() {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.snap.State = StateCompleted
	if j.snap.Failed > 0 {
		j.snap.State = StateCompletedWithErrors
	}
}

// view returns a copy of the counters and up to limit errors.
func (j *Job) view(limit int) (Snapshot, []RowError) {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.snap, append([]RowError{}, j.errs[:min(limit, len(j.errs))]...)
}

// pending returns the snapshot and the not-yet-stored errors; commit(n)
// marks them stored once the store accepted them.
func (j *Job) pending() (snap Snapshot, newErrs []RowError, commit func()) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.snap.UpdatedAt = time.Now().UTC()
	n := len(j.errs)
	newErrs = append([]RowError(nil), j.errs[j.flushed:n]...)
	return j.snap, newErrs, func() {
		j.mu.Lock()
		defer j.mu.Unlock()
		j.flushed = n
	}
}
