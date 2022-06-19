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
	// SQLDriverKey SqlDriver key for transaction internal driver list.
	SQLDriverKey = "sql"
)

// Driver database implementation of underlying commit and rollback mechanisms.
type Driver interface {
	// Exec proceeds with the execution of the set of Statement from a transaction context.
	Exec(context.Context, []Statement) error
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

func (n NoopDriver) Exec(_ context.Context, _ []Statement) error {
	return nil
}
