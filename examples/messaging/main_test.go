package main

import "testing"

func TestBrokerSelectionAndRequiredAddresses(t *testing.T) {
	brokers, err := selectBrokers("all")
	if err != nil || len(brokers) != 3 {
		t.Fatal(brokers, err)
	}
	for _, broker := range brokers {
		names, err := selectBrokers(broker)
		if err != nil || len(names) != 1 || names[0] != broker {
			t.Fatal(names, err)
		}
	}
	if _, err := selectBrokers("unknown"); err == nil {
		t.Fatal("unknown broker accepted")
	}
	t.Setenv("EVENTBUS_REDIS_ADDR", "")
	t.Setenv("EVENTBUS_KAFKA_BROKERS", "")
	for _, broker := range []string{"redis", "kafka", "all", "unknown"} {
		if err := envExampleConfig(t).validateBroker(broker); err == nil {
			t.Fatalf("invalid/missing %s fixture accepted", broker)
		}
	}
	if err := envExampleConfig(t).validateBroker("nats"); err != nil {
		t.Fatal(err)
	}
}

func TestDriverSelectionAndRequiredAddresses(t *testing.T) {
	drivers, err := selectDrivers("all")
	if err != nil || len(drivers) != 3 {
		t.Fatal(drivers, err)
	}
	for _, driver := range drivers {
		names, err := selectDrivers(driver)
		if err != nil || len(names) != 1 || names[0] != driver {
			t.Fatal(names, err)
		}
	}
	if _, err := selectDrivers("unknown"); err == nil {
		t.Fatal("unknown database driver accepted")
	}
	t.Setenv("ORM_POSTGRES_DSN", "")
	t.Setenv("ORM_MYSQL_DSN", "")
	t.Setenv("MONGODB_INTEGRATION_URI", "")
	for _, driver := range []string{"postgres", "mysql", "mongodb", "all", "unknown"} {
		if err := envExampleConfig(t).validateDriver(driver); err == nil {
			t.Fatalf("invalid/missing %s database accepted", driver)
		}
	}
	t.Setenv("ORM_POSTGRES_DSN", "fixture-postgres")
	t.Setenv("ORM_MYSQL_DSN", "fixture-mysql")
	t.Setenv("MONGODB_INTEGRATION_URI", "fixture-mongodb")
	for _, driver := range drivers {
		if err := envExampleConfig(t).validateDriver(driver); err != nil {
			t.Fatal(err)
		}
	}
}
