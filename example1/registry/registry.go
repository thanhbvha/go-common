package registry

import (
	"github.com/thanhbvha/go-common/queue"
)

// TaskRegistration holds the configuration and handler for a job type.
type TaskRegistration struct {
	JobType string
	Options queue.JobTypeOptions
	Handler queue.JobHandler
}

// Global list of registered tasks
var registeredTasks []TaskRegistration

// Register adds a new task configuration to the registry.
// This is typically called from the init() functions in your task packages.
func Register(jobType string, opts queue.JobTypeOptions, handler queue.JobHandler) {
	registeredTasks = append(registeredTasks, TaskRegistration{
		JobType: jobType,
		Options: opts,
		Handler: handler,
	})
}

// ApplyToQueue registers all autoloaded tasks into the provided queue instance.
func ApplyToQueue(q *queue.Queue) {
	for _, task := range registeredTasks {
		q.RegisterJobType(task.JobType, task.Options)
		q.RegisterHandler(task.JobType, task.Handler)
	}
}
