package mongodb

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Capabilities describe server support, not the authenticated user's permissions.
type Capabilities struct {
	Topology        string
	ReplicaSetName  string
	LogicalSessions bool
	Transactions    bool
	MaxWireVersion  int32
}

func DetectCapabilities(ctx context.Context, client *mongo.Client) (Capabilities, error) {
	var hello struct {
		SetName        string `bson:"setName"`
		Msg            string `bson:"msg"`
		Sessions       *int64 `bson:"logicalSessionTimeoutMinutes"`
		MaxWireVersion int32  `bson:"maxWireVersion"`
	}
	if err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello); err != nil {
		return Capabilities{}, err
	}
	c := Capabilities{Topology: "standalone", ReplicaSetName: hello.SetName, LogicalSessions: hello.Sessions != nil, MaxWireVersion: hello.MaxWireVersion}
	if hello.SetName != "" {
		c.Topology = "replicaSet"
		c.Transactions = c.LogicalSessions && c.MaxWireVersion >= 7
	}
	if hello.Msg == "isdbgrid" {
		c.Topology = "sharded"
		c.Transactions = c.LogicalSessions && c.MaxWireVersion >= 8
	}
	return c, nil
}

// Capabilities performs a fresh read-only hello against an alias's client.
func (m *Manager) Capabilities(ctx context.Context, alias string) (Capabilities, error) {
	db, err := m.Lookup(alias)
	if err != nil {
		return Capabilities{}, err
	}
	return DetectCapabilities(ctx, db.Client())
}
func GetCapabilities(ctx context.Context, alias string) (Capabilities, error) {
	return globalManager.Capabilities(ctx, alias)
}
