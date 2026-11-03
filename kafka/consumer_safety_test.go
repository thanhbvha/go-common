package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

type fakeSubscription struct {
	polls           int
	records         []*kgo.Record
	committed       []*kgo.Record
	commitErr       error
	allowed, closed bool
}

func (f *fakeSubscription) PollRecords(ctx context.Context, n int) kgo.Fetches {
	f.polls++
	if f.polls > 1 {
		return kgo.Fetches{{Topics: []kgo.FetchTopic{{Topic: "jobs", Partitions: []kgo.FetchPartition{{Err: context.Canceled}}}}}}
	}
	return kgo.Fetches{{Topics: []kgo.FetchTopic{{Topic: "jobs", Partitions: []kgo.FetchPartition{{Records: f.records}}}}}}
}
func (f *fakeSubscription) CommitRecords(_ context.Context, records ...*kgo.Record) error {
	f.committed = append(f.committed, records...)
	return f.commitErr
}
func (f *fakeSubscription) AllowRebalance()         { f.allowed = true }
func (f *fakeSubscription) CloseAllowingRebalance() { f.closed = true }
func TestSubscriptionStopsAtFailedOffsetAndCommitsOnlySuccessPrefix(t *testing.T) {
	failure := errors.New("business failure")
	f := &fakeSubscription{records: []*kgo.Record{{Topic: "jobs", Offset: 0}, {Topic: "jobs", Offset: 1}, {Topic: "jobs", Offset: 2}}}
	var calls []int64
	err := consumeSubscription(context.Background(), f, 100, func(ctx context.Context, records []*kgo.Record) (int, error) {
		for i, r := range records {
			calls = append(calls, r.Offset)
			if r.Offset == 1 {
				return i, failure
			}
		}
		return len(records), nil
	})
	if !errors.Is(err, failure) || len(calls) != 2 || len(f.committed) != 1 || f.committed[0].Offset != 0 || !f.closed {
		t.Fatalf("err=%v calls=%v commits=%v closed=%v", err, calls, f.committed, f.closed)
	}
}
func TestSubscriptionFailedBatchDoesNotCommitOrPollAgain(t *testing.T) {
	failure := errors.New("batch failure")
	f := &fakeSubscription{records: []*kgo.Record{{Topic: "jobs", Offset: 3}, {Topic: "jobs", Offset: 4}}}
	err := consumeSubscription(context.Background(), f, 2, func(context.Context, []*kgo.Record) (int, error) { return 0, failure })
	if !errors.Is(err, failure) || len(f.committed) != 0 || f.polls != 1 || !f.closed {
		t.Fatalf("err=%v commits=%v polls=%d", err, f.committed, f.polls)
	}
}
func TestSubscriptionCommitFailureStopsIntake(t *testing.T) {
	failure := errors.New("commit failure")
	f := &fakeSubscription{records: []*kgo.Record{{Topic: "jobs", Offset: 3}}, commitErr: failure}
	err := consumeSubscription(context.Background(), f, 100, func(_ context.Context, r []*kgo.Record) (int, error) { return len(r), nil })
	if !errors.Is(err, failure) || f.polls != 1 || !f.closed {
		t.Fatalf("err=%v polls=%d closed=%v", err, f.polls, f.closed)
	}
}
func TestKafkaClientCloseIsTerminal(t *testing.T) {
	c := New(DefaultConfig())
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.Connect(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Connect after Close: %v", err)
	}
}

func TestHealthFailureRetainsNativeClientAndIndependentSubscriptions(t *testing.T) {
	config := DefaultConfig()
	config.ConsumerGroup = "workers"
	c := New(config)
	native, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	c.kgo = native
	c.connected = false
	defer c.Close()
	if err = c.Connect(context.Background()); err != nil {
		t.Fatalf("health failure caused a new dial: %v", err)
	}
	if c.Native() != native {
		t.Fatal("native lifecycle replaced by health status")
	}
	first, err := c.openSubscription([]string{"first"})
	if err != nil {
		t.Fatal(err)
	}
	defer first.CloseAllowingRebalance()
	second, err := c.openSubscription([]string{"second"})
	if err != nil {
		t.Fatal(err)
	}
	defer second.CloseAllowingRebalance()
	if first == second || first == native || second == native {
		t.Fatal("subscriptions shared native clients")
	}
	if first.GetConsumeTopics()[0] != "first" || second.GetConsumeTopics()[0] != "second" || len(native.GetConsumeTopics()) != 0 {
		t.Fatal("subscription topics leaked")
	}
	assertKafkaOpt(t, first, kgo.DisableAutoCommit, true)
	assertKafkaOpt(t, first, kgo.BlockRebalanceOnPoll, true)
}
