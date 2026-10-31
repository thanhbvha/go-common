package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/thanhbvha/go-common/db/mongodb"
)

func (s *exampleState) capabilities(ctx context.Context) error {
	db, err := s.manager.Lookup("orders")
	if err != nil {
		return err
	}
	managed, err := s.manager.Capabilities(ctx, "orders")
	if err != nil {
		return err
	}
	native, err := mongodb.DetectCapabilities(ctx, db.Client())
	if err != nil {
		return err
	}
	if !managed.Transactions || !managed.LogicalSessions || managed != native {
		return errors.New("unexpected transaction capabilities")
	}
	fmt.Fprintf(s.out, "  topology=%s sessions=%v transactions=%v wire_version=%d\n", managed.Topology, managed.LogicalSessions, managed.Transactions, managed.MaxWireVersion)
	fmt.Fprintln(s.out, "  RequireTransactions checked startup; capability support does not grant database permissions")
	return nil
}

func (s *exampleState) diagnostics(ctx context.Context) error {
	invalid := s.configs["orders"]
	invalid.TransactionTimeout = -time.Second
	err := s.manager.AddConnection(ctx, "invalid-timeout", invalid, false)
	var configError *mongodb.ConfigError
	if !errors.Is(err, mongodb.ErrInvalidConfig) || !errors.As(err, &configError) || configError.Field != "transaction_timeout" {
		return errors.New("missing typed timeout diagnosis")
	}
	fmt.Fprintf(s.out, "  config: field=%s code=%s\n", configError.Field, configError.Code)
	invalid = s.configs["orders"]
	invalid.URI = "mongodb://localhost/?maxPoolSize=invalid"
	err = s.manager.AddConnection(ctx, "invalid-uri", invalid, false)
	if !errors.As(err, &configError) {
		return errors.New("missing typed URI diagnosis")
	}
	fmt.Fprintf(s.out, "  config: field=%s code=%s (URI value omitted)\n", configError.Field, configError.Code)
	conflicting := s.configs["orders"]
	conflicting.OperationTimeout += time.Second
	err = s.manager.AddConnection(ctx, "conflicting-group", conflicting, false)
	var groupError *mongodb.ClientGroupError
	if !errors.Is(err, mongodb.ErrClientGroupConflict) || !errors.As(err, &groupError) {
		return errors.New("missing group conflict diagnosis")
	}
	fmt.Fprintf(s.out, "  client group: conflicting fields=%v (values omitted)\n", groupError.Fields)
	for _, alias := range []string{"invalid-timeout", "invalid-uri", "conflicting-group"} {
		if _, err := s.manager.Lookup(alias); err == nil {
			return errors.New("invalid configuration published an alias")
		}
	}
	return nil
}
