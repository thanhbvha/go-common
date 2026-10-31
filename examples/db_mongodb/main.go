package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/thanhbvha/go-common/db/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
)

var exampleNames = []string{"connections", "capabilities", "diagnostics", "crud", "options", "pagination", "stream", "watch", "transaction", "retry", "observability", "guards", "lifecycle", "drain", "global"}

type exampleState struct {
	manager     *mongodb.Manager
	configs     map[string]mongodb.Config
	databases   []string // Generated here, never supplied by the URI, flags or environment.
	out         io.Writer
	poolCreated atomic.Int64
	metrics     exampleMetrics
}

// stageError preserves errors.Is without exposing a connection URI or document
// through command-line output. Production applications should sanitize diagnostics too.
type stageError struct {
	stage string
	cause error
}

func (e *stageError) Error() string {
	return "MongoDB example failed at " + e.stage + "; connection details and documents omitted"
}
func (e *stageError) Unwrap() error { return e.cause }

func main() {
	selected := flag.String("example", "all", "all, "+strings.Join(exampleNames, ", "))
	flag.Parse()
	uri := os.Getenv("MONGODB_URI")
	if uri == "" {
		uri = "mongodb://localhost:27017"
	} // Local development only.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := runExample(ctx, uri, *selected, os.Stdout); err != nil {
		if errors.Is(err, mongodb.ErrTransactionsUnsupported) {
			fmt.Fprintln(os.Stderr, "MongoDB example requires a replica set or sharded deployment; standalone does not support transactions")
			os.Exit(1)
		}
		var stage *stageError
		if errors.As(err, &stage) {
			fmt.Fprintln(os.Stderr, stage.Error())
		} else {
			fmt.Fprintln(os.Stderr, "MongoDB example failed; details omitted")
		}
		os.Exit(1)
	}
}

func runExample(ctx context.Context, uri, selected string, out io.Writer) (result error) {
	known := selected == "all"
	for _, name := range exampleNames {
		known = known || name == selected
	}
	if !known {
		return &stageError{stage: "argument validation", cause: errors.New("unknown example selection")}
	}
	s := &exampleState{manager: mongodb.NewManager(), out: out}
	runID := bson.NewObjectID().Hex()
	monitor := &event.PoolMonitor{Event: func(e *event.PoolEvent) {
		if e.Type == event.ConnectionPoolCreated {
			s.poolCreated.Add(1)
		}
	}}
	base := mongodb.Config{
		URI: uri, ClientGroup: "business", AppName: "go-common-mongodb-example",
		MaxPoolSize: 20, ConnectTimeout: 5 * time.Second, PingTimeout: 5 * time.Second,
		OperationTimeout: 5 * time.Second, TransactionTimeout: 10 * time.Second,
		RequireTransactions: true,
		PoolMonitor:         monitor,
		// Share the same monitor pointer across every binding in this group.
		TransactionMonitor: &mongodb.TransactionMonitor{Event: s.metrics.record},
	}
	s.configs = make(map[string]mongodb.Config)
	for _, alias := range []string{"orders", "inventory", "audit"} {
		cfg := base
		cfg.DBName = "go_common_example_" + runID + "_" + alias
		s.configs[alias] = cfg
		s.databases = append(s.databases, cfg.DBName)
	}
	// Init binds two databases to one client; AddConnection joins a third below.
	if err := s.manager.Init(ctx, map[string]mongodb.Config{
		"orders": s.configs["orders"], "inventory": s.configs["inventory"],
	}, "orders"); err != nil {
		return &stageError{stage: "startup", cause: err}
	}
	defer func() {
		if err := s.cleanup(); err != nil {
			fmt.Fprintln(out, "cleanup: FAILED; inspect temporary example databases")
			result = errors.Join(result, &stageError{stage: "cleanup", cause: err})
		} else {
			fmt.Fprintln(out, "cleanup: temporary databases removed and clients closed")
		}
	}()
	if err := s.manager.AddConnection(ctx, "audit", s.configs["audit"], false); err != nil {
		return &stageError{stage: "audit binding", cause: err}
	}
	// Same URI, explicitly independent client: it cannot join a business transaction.
	isolated := s.configs["inventory"]
	isolated.ClientGroup = ""
	if err := s.manager.AddConnection(ctx, "isolated", isolated, false); err != nil {
		return &stageError{stage: "isolated connection", cause: err}
	}
	if err := s.seed(ctx); err != nil {
		return &stageError{stage: "fixture setup", cause: err}
	}
	steps := []struct {
		name string
		fn   func(context.Context) error
	}{
		{"connections", s.connections}, {"capabilities", s.capabilities}, {"diagnostics", s.diagnostics},
		{"crud", s.crud}, {"options", s.driverOptions}, {"pagination", s.pagination},
		{"stream", s.stream}, {"watch", s.watch}, {"transaction", s.transactions}, {"retry", s.retry}, {"observability", s.observability},
		{"guards", s.guards}, {"lifecycle", s.lifecycle}, {"drain", s.drain}, {"global", s.global},
	}
	for _, step := range steps {
		if selected != "all" && selected != step.name {
			continue
		}
		if err := step.fn(ctx); err != nil {
			return &stageError{stage: step.name, cause: err}
		}
		fmt.Fprintf(out, "%s: OK\n", step.name)
	}
	return nil
}

func (s *exampleState) connections(ctx context.Context) error {
	orders, err := s.manager.Lookup("")
	if err != nil {
		return err
	}
	inventory, err := s.manager.Lookup("inventory")
	if err != nil {
		return err
	}
	audit, err := s.manager.Lookup("audit")
	if err != nil {
		return err
	}
	isolated, err := s.manager.Lookup("isolated")
	if err != nil {
		return err
	}
	if orders.Client() != inventory.Client() || orders.Client() != audit.Client() || orders.Client() == isolated.Client() {
		return errors.New("unexpected client ownership")
	}
	fmt.Fprintln(s.out, "  3 databases share business client; isolated alias owns a different client")
	// Pool count is per server, not the number of MongoDB clients.
	fmt.Fprintf(s.out, "  driver pool-created events observed: %d\n", s.poolCreated.Load())
	if err := orders.Client().Ping(ctx, nil); err != nil {
		return err
	}
	caps, err := s.manager.Capabilities(ctx, "orders")
	if err != nil {
		return err
	}
	fmt.Fprintf(s.out, "  topology=%s transactions=%v\n", caps.Topology, caps.Transactions)
	return nil
}

func (s *exampleState) cleanup() error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var failures []error
	for _, name := range s.databases {
		if !strings.HasPrefix(name, "go_common_example_") {
			failures = append(failures, errors.New("refusing unsafe database cleanup"))
			continue
		}
		// The default owner remains connected until every generated database is dropped.
		db, err := s.manager.Lookup("orders")
		if err != nil {
			failures = append(failures, err)
			continue
		}
		if err := db.Client().Database(name).Drop(ctx); err != nil {
			failures = append(failures, err)
		}
	}
	shutdown, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	failures = append(failures, s.manager.DisconnectAll(shutdown))
	return errors.Join(failures...)
}
