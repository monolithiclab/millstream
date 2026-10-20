package backtest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/monolithiclab/millstream/internal/patch"
)

// MaxJobs is how many backtests Jobs remembers, the oldest finished one forgotten first.
const MaxJobs = 100

// Job states.
const (
	JobRunning = "running"
	JobDone    = "done"
	JobFailed  = "failed"
)

// Job is a backtest run in the background.
type Job struct {
	ID       string
	Pipeline string
	State    string
	// Err is why a failed job failed; Report is a done job's.
	Err        error
	Report     *Report
	CreatedAt  time.Time
	FinishedAt time.Time
}

// preparer is a Runner that can refuse a request before running it (Backtester.Prepare).
type preparer interface {
	Prepare(req Request) (*patch.Patch, error)
}

// Jobs runs backtests in the background for the API (POST /api/v1/backtests answers at once): Start returns the job,
// Get its state and, once done, its report. Jobs live in the process's memory: a restart forgets them.
type Jobs struct {
	ctx context.Context
	r   Runner
	now func() time.Time

	mu    sync.Mutex
	jobs  map[string]*Job
	order []string
	wg    sync.WaitGroup
}

// NewJobs returns Jobs running r; ctx ends the backtests still running, now is the clock of their instants (nil
// means time.Now).
func NewJobs(ctx context.Context, r Runner, now func() time.Time) *Jobs {
	if now == nil {
		now = time.Now
	}
	return &Jobs{ctx: ctx, r: r, now: now, jobs: map[string]*Job{}}
}

// Start starts a backtest, refusing at once what the runner can refuse without running it.
func (j *Jobs) Start(req Request) (Job, error) {
	if p, ok := j.r.(preparer); ok {
		if _, err := p.Prepare(req); err != nil {
			return Job{}, err
		}
	}
	var b [8]byte
	_, _ = rand.Read(b[:])
	job := &Job{ID: "bt_" + hex.EncodeToString(b[:]), Pipeline: req.Pipeline, State: JobRunning,
		CreatedAt: j.now().UTC()}
	j.mu.Lock()
	j.jobs[job.ID] = job
	j.order = append(j.order, job.ID)
	j.forget()
	snapshot := *job
	j.mu.Unlock()
	j.wg.Add(1)
	go func() {
		defer j.wg.Done()
		rep, err := j.r.Run(j.ctx, req)
		j.mu.Lock()
		defer j.mu.Unlock()
		job.FinishedAt, job.Report, job.Err, job.State = j.now().UTC(), rep, err, JobDone
		if err != nil {
			job.State = JobFailed
		}
	}()
	return snapshot, nil
}

// forget drops the oldest finished jobs past MaxJobs.
func (j *Jobs) forget() {
	for i := 0; len(j.order) > MaxJobs && i < len(j.order); {
		if id := j.order[i]; j.jobs[id].State != JobRunning {
			delete(j.jobs, id)
			j.order = slices.Delete(j.order, i, i+1)
			continue
		}
		i++
	}
}

// Get returns a job.
func (j *Jobs) Get(id string) (Job, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	job, ok := j.jobs[id]
	if !ok {
		return Job{}, false
	}
	return *job, true
}

// Wait waits for the backtests started so far to end.
func (j *Jobs) Wait() { j.wg.Wait() }

// trainer is a Runner that serves the distill proposal's training sets (Backtester.Training).
type trainer interface {
	Training(ctx context.Context, pipelineName, step string) (Training, error)
}

// ErrNoTraining is returned by Jobs.Training when its runner serves no training sets.
var ErrNoTraining = errors.New("this engine serves no distill training sets")

// Training returns the training side of an agent step's split, from the runner (Backtester.Training).
func (j *Jobs) Training(ctx context.Context, pipelineName, step string) (Training, error) {
	t, ok := j.r.(trainer)
	if !ok {
		return Training{}, ErrNoTraining
	}
	return t.Training(ctx, pipelineName, step)
}
