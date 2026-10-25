package demo

import (
	"context"
	"errors"
	"fmt"
	"net/mail"
	"strings"
	"time"

	"github.com/thanhbvha/go-common/eventbus"
)

type UserMessage struct {
	UserID string `json:"user_id"`
	Email  string `json:"email"`
	Mode   string `json:"mode,omitempty"`
}

func (m UserMessage) Validate() error {
	if strings.TrimSpace(m.UserID) == "" {
		return fmt.Errorf("user_id is required")
	}
	if _, err := mail.ParseAddress(m.Email); err != nil {
		return fmt.Errorf("email is invalid")
	}
	switch m.Mode {
	case "", "ack", "retry", "permanent", "skip", "slow":
		return nil
	default:
		return fmt.Errorf("unsupported mode %q", m.Mode)
	}
}

func TaskHandler(logger *Logger) eventbus.Handler {
	return eventbus.Handle(func(ctx context.Context, job eventbus.Job, payload UserMessage) error {
		if err := payload.Validate(); err != nil {
			return eventbus.MarkPermanent(err)
		}
		logger.Info("task received", "job_id", job.ID(), "attempt", job.Attempt(), "max_attempts", job.MaxAttempts(), "traceparent", job.Header("traceparent"))
		switch payload.Mode {
		case "retry":
			if job.Attempt() == 1 {
				return eventbus.MarkRetryAfter(errors.New("synthetic temporary SMTP failure"), time.Second)
			}
		case "permanent":
			return eventbus.MarkPermanent(errors.New("synthetic rejected recipient"))
		case "skip":
			return eventbus.MarkSkip("synthetic user opted out")
		case "slow":
			select {
			case <-time.After(2 * time.Second):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		logger.Info("task completed", "job_id", job.ID(), "user_id", payload.UserID)
		return nil
	})
}

func EventHandler(name string, logger *Logger) eventbus.Handler {
	return eventbus.Handle(func(_ context.Context, job eventbus.Job, payload UserMessage) error {
		if err := payload.Validate(); err != nil {
			return eventbus.MarkPermanent(err)
		}
		logger.Info("event received", "subscriber", name, "job_id", job.ID(), "user_id", payload.UserID)
		return nil
	})
}

func DeclareTask(bus eventbus.Bus, driver string, withHandler bool, logger *Logger) error {
	if err := bus.DeclareTask(TaskName, TopologyOptions(driver)...); err != nil {
		return err
	}
	if withHandler {
		return bus.HandleTask(TaskName, TaskHandler(logger))
	}
	return nil
}

func DeclareEvent(bus eventbus.Bus, driver string) error {
	return bus.DeclareEvent(EventName, EventTopologyOptions(driver)...)
}
