package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/thanhbvha/go-common/db/mongodb"
)

func (s *exampleState) lifecycle(ctx context.Context) (result error) {
	// A separate owner demonstrates restart without interrupting the main example.
	manager := mongodb.NewManager()
	defer func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		result = errors.Join(result, manager.DisconnectAll(shutdown))
	}()
	if err := manager.Init(ctx, s.configs, "orders"); err != nil {
		return err
	}
	before, err := manager.Lookup("orders")
	if err != nil {
		return err
	}
	if err := manager.Init(ctx, s.configs, "orders"); err != nil {
		return err
	}
	again, err := manager.Lookup("orders")
	if err != nil {
		return err
	}
	if before.Client() != again.Client() {
		return errors.New("repeated Init replaced the client")
	}
	// Real services stop their handlers/workers before DisconnectAll.
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	err = manager.DisconnectAll(shutdown)
	cancel()
	if err != nil {
		return err
	}
	if _, err := manager.Lookup("orders"); err == nil {
		return errors.New("closed handle still published")
	}
	if err := manager.Init(ctx, s.configs, "orders"); err != nil {
		return err
	}
	after, err := manager.Lookup("orders")
	if err != nil {
		return err
	}
	if before.Client() == after.Client() {
		return errors.New("reinitialization reused closed client")
	}
	if err := after.Client().Ping(ctx, nil); err != nil {
		return err
	}
	fmt.Fprintln(s.out, "  repeated Init kept client; shutdown cleared handles; reinitialization created a working client")
	return nil
}
