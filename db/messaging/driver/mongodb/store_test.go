package mongodb

import (
	"testing"

	"github.com/thanhbvha/go-common/v2/db/messaging/inbox"
)

func TestIdentityScopesAndConfiguration(t *testing.T) {
	if identity("a", "b", "c") == identity("a:b", "", "c") {
		t.Fatal("ambiguous tuple encoding")
	}
	a := inbox.Receipt{}
	a.Namespace = "n"
	a.ID = "id"
	a.Consumer = "one"
	b := a
	b.Consumer = "two"
	if receiptID(a) == receiptID(b) {
		t.Fatal("consumer scope missing")
	}
	for _, cfg := range []Config{{}, {Namespace: "n", Alias: "a", Prefix: "bad.name"}, {Namespace: "n", Alias: "a", TransactionAliases: []string{""}}} {
		if _, err := configuration(cfg); err == nil {
			t.Fatal("invalid config accepted")
		}
	}
}
