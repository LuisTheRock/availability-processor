package imports

import (
	"context"
	cryptorand "crypto/rand"
	"fmt"
	"sync"
	"time"

	"availability-processor/internal/availability"
	"availability-processor/internal/upstream"

	"go.uber.org/zap"
)

const (
	rowConcurrency = 8
	flushEvery     = time.Second
)

type contextKey struct{ product, size, country string }

// Service runs imports. Memory holds the live copy of running jobs; the store
// (Redis) holds the durable copy, written every flushEvery and on completion.
type Service struct {
	proc   *availability.Processor
	client *upstream.Client
	store  jobStore
	log    *zap.Logger

	ctx  context.Context // cancelled on shutdown; owns every worker goroutine
	wg   sync.WaitGroup
	rows chan struct{} // one slot per row in flight, shared by all jobs on this pod

	mu   sync.Mutex
	jobs map[string]*Job
}

func newService(proc *availability.Processor, client *upstream.Client, store jobStore, log *zap.Logger) *Service {
	return &Service{proc: proc, client: client, store: store, log: log, ctx: context.Background(), rows: make(chan struct{}, rowConcurrency), jobs: map[string]*Job{}}
}

// Submit registers the job durably, then processes it in the background.
// parseErrs are rows that were malformed in the upload.
func (s *Service) Submit(ctx context.Context, rows []Row, parseErrs []RowError) (string, error) {
	job := newJob(cryptorand.Text(), len(rows)+len(parseErrs))
	for _, e := range parseErrs {
		job.fail(e.Row, e.Code, e.Message)
	}

	if err := s.flush(ctx, job); err != nil { // durable before the client sees the id
		return "", err
	}

	s.mu.Lock()
	s.jobs[job.snap.ID] = job
	s.mu.Unlock()

	s.wg.Go(func() { s.run(job, rows) })
	return job.snap.ID, nil
}

// Get serves the freshest view: memory while the job runs here, else the store.
func (s *Service) Get(ctx context.Context, id string, limit int) (Snapshot, []RowError, error) {
	s.mu.Lock()
	job, ok := s.jobs[id]
	s.mu.Unlock()
	if ok {
		sn, errs := job.view(limit)
		return sn, errs, nil
	}
	return s.store.Load(ctx, id, limit)
}

func (s *Service) flush(ctx context.Context, job *Job) error {
	sn, newErrs, commit := job.pending()
	if err := s.store.Save(ctx, sn, newErrs); err != nil {
		return err
	}
	commit()
	return nil
}

func (s *Service) run(job *Job, rows []Row) {
	job.setState(StateRunning)

	stop := make(chan struct{})
	var ticker sync.WaitGroup
	ticker.Go(func() {
		t := time.NewTicker(flushEvery)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				if err := s.flush(s.ctx, job); err != nil {
					s.log.Warn("flushing job status, will retry", zap.String("jobId", job.snap.ID), zap.Error(err))
				}
			case <-stop:
				return
			case <-s.ctx.Done():
				return
			}
		}
	})

	s.process(job, rows)

	close(stop)
	ticker.Wait()

	if s.ctx.Err() != nil {
		job.setState(StateInterrupted)
	} else {
		job.finish()
	}

	// Detach from the cancelled shutdown context so the final state is saved.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(s.ctx), 5*time.Second)
	defer cancel()
	if err := s.flush(ctx, job); err != nil {
		s.log.Error("saving final job status; keeping it in memory", zap.String("jobId", job.snap.ID), zap.Error(err))
		return
	}
	s.mu.Lock()
	delete(s.jobs, job.snap.ID) // reads now come from the store
	s.mu.Unlock()
}

func (s *Service) process(job *Job, rows []Row) {
	index, snapErrs := s.loadSnapshots(rows)

	lookup := func(_ context.Context, c availability.Change) (upstream.ProductContext, error) {
		if err := snapErrs[c.CountryCode]; err != nil {
			return upstream.ProductContext{}, err
		}
		pc, ok := index[contextKey{c.ProductID, c.SizeID, c.CountryCode}]
		if !ok {
			return upstream.ProductContext{}, upstream.ErrNotFound
		}
		return pc, nil
	}

	var inFlight sync.WaitGroup
	for _, r := range rows {
		select {
		case s.rows <- struct{}{}:
		case <-s.ctx.Done():
			inFlight.Wait()
			return
		}
		inFlight.Go(func() {
			defer func() { <-s.rows }()
			// Detached: a row already started finishes (PUT, record, publish) even
			// during shutdown; only the launch of new rows stops with s.ctx.
			res, err := s.proc.Process(context.WithoutCancel(s.ctx), r.Change, lookup)
			job.record(r.Num, res, err)
		})
	}
	inFlight.Wait()
}

// loadSnapshots fetches one snapshot per distinct country and indexes it:
// O(S) time and space to build, then O(1) per row (O(S+R) overall).
func (s *Service) loadSnapshots(rows []Row) (map[contextKey]upstream.ProductContext, map[string]error) {
	countries := map[string]struct{}{}
	for _, r := range rows {
		countries[r.Change.CountryCode] = struct{}{}
	}

	index := map[contextKey]upstream.ProductContext{}
	errs := map[string]error{}
	for country := range countries {
		snap, err := s.client.Snapshot(s.ctx, country)
		if err != nil {
			errs[country] = fmt.Errorf("snapshot unavailable: %w", err)
			continue
		}
		for _, pc := range snap {
			index[contextKey{pc.ProductID, pc.SizeID, pc.CountryCode}] = pc
		}
	}
	return index, errs
}

// Wait blocks until running jobs have saved their final state.
func (s *Service) Wait() { s.wg.Wait() }
