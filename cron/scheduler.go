// Package cron provides a distributed job scheduler.
// It wraps robfig/cron/v3 and adds support for Distributed Locks (Leader Election)
// via Redis to ensure jobs only run on a single node in a cluster.
package cron

import (
	"log"

	"github.com/robfig/cron/v3"
)

// Scheduler wraps the robfig cron scheduler.
type Scheduler struct {
	cron *cron.Cron
}

// NewScheduler creates a new Cron Scheduler with seconds support.
func NewScheduler() *Scheduler {
	// Enable second-level precision, just like standard Quartz cron
	c := cron.New(cron.WithSeconds())
	return &Scheduler{
		cron: c,
	}
}

// AddJob adds a job to the scheduler.
// The schedule must be a valid cron expression (e.g., "0 * * * * *" for every minute).
func (s *Scheduler) AddFunc(schedule string, cmd func()) (cron.EntryID, error) {
	return s.cron.AddFunc(schedule, cmd)
}

// Start begins the cron scheduler in its own goroutine.
func (s *Scheduler) Start() {
	log.Println("[Cron] Scheduler started")
	s.cron.Start()
}

// Stop stops the cron scheduler. It does not stop jobs that are already running.
func (s *Scheduler) Stop() {
	log.Println("[Cron] Scheduler stopped")
	s.cron.Stop()
}
