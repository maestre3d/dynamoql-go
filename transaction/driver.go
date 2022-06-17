package transaction

import (
	"context"
	"sync"
)

const (
	// NoopDriverKey NoopDriver key for transaction internal driver list.
	NoopDriverKey = "noop"
	// DynamoDBDriverKey DynamoDBDriver key for transaction internal driver list.
	DynamoDBDriverKey = "dynamodb"
)

// Driver database implementation of underlying commit and rollback mechanisms.
type Driver interface {
	// Commit proceeds with the execution of the set of Statement from a transaction context.
	Commit(context.Context, []Statement) error
	// Rollback cancels the execution of the set of Statement from a transaction context.
	Rollback(context.Context, []Statement) error
}

var (
	// driversMu guarantees drivers atomicity in concurrent scenarios.
	driversMu sync.RWMutex
	// drivers a list of available Driver(s) previously registered manually by a system.
	//
	// Set No-op driver automatically.
	drivers = map[string]Driver{
		NoopDriverKey: NoopDriver{},
	}
)

// RegisterDriver sets a database driver for transaction contexts.
//
// If called with a driver equals to nil, it panics.
func RegisterDriver(name string, driver Driver) {
	driversMu.Lock()
	defer driversMu.Unlock()
	if driver == nil {
		panic(ErrMissingDriver)
	}
	drivers[name] = driver
}

// NoopDriver no-operation Driver.
type NoopDriver struct{}

var _ Driver = NoopDriver{}

func (n NoopDriver) Commit(_ context.Context, _ []Statement) error {
	return nil
}

func (n NoopDriver) Rollback(_ context.Context, _ []Statement) error {
	return nil
}
