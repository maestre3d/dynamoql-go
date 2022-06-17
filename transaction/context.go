package transaction

import (
	"context"
	"math/rand"
	"sync"
)

// ContextKeyType custom-type of the key for transaction identifiers stored in context.Context.
type ContextKeyType string

const (
	// ContextKey key for transaction identifiers stored in context.Context.
	ContextKey ContextKeyType = "transaction_context"
)

var (
	// GlobalDriver default Driver to be used by commit and rollback operations.
	GlobalDriver = NoopDriverKey
	// MaxTransactionStatements the maximum amount of statements per-transaction.
	//
	// It is very important to set this variable properly as internal `Transaction Context` components
	// use it to pre-allocate memory (slice of statements of each transaction), increasing overall performance.
	//
	// Default is Amazon DynamoDB's limit (25 statements per-transaction).
	MaxTransactionStatements = 25

	// internalRegistry Map which keeps track of transactions statements using transaction identifiers as key
	// and a slice of Statement as value.
	internalRegistry *sync.Map
)

// Context a mapping of an id and its Driver. Driver is required to perform commit and rollback operations.
type Context struct {
	ID int
	// Using string instead Driver as this will decrease overall struct copies.
	Driver string
}

// NewContext builds a context.Context with a transaction identifier from a parent context.
// If given parent context is nil, returns nil.
//
// Finally, if given a context.Context with a Context already registered, this will override the entry.
func NewContext(ctx context.Context) context.Context {
	return NewContextWithDriver(ctx, GlobalDriver)
}

// NewContextWithDriver builds a context.Context with a transaction identifier and a scoped driver key from a
// parent context. If given parent context is nil, returns nil.
//
// Finally, if given a context.Context with a Context already registered, this will override the entry.
func NewContextWithDriver(ctx context.Context, driver string) context.Context {
	if ctx == nil {
		return nil
	}
	return context.WithValue(ctx, ContextKey, Context{
		ID:     rand.Int(),
		Driver: driver,
	})
}

// GetID returns a transaction identifier from context.Context. Returns ErrMissingID if missing.
func GetID(ctx context.Context) (int, error) {
	if ctx == nil {
		return 0, ErrMissingContext
	}
	txCtx, ok := ctx.Value(ContextKey).(Context)
	if !ok {
		return 0, ErrMissingContext
	}
	return txCtx.ID, nil
}

// GetContext returns a transaction context from context.Context.
//
// Returns ErrMissingDriver if no driver was found in context.
func GetContext(ctx context.Context) (Context, error) {
	if ctx == nil {
		return Context{}, ErrMissingContext
	}
	txCtx, ok := ctx.Value(ContextKey).(Context)
	if !ok {
		return Context{}, ErrMissingContext
	} else if _, ok = drivers[txCtx.Driver]; !ok {
		return Context{}, ErrMissingDriver
	}
	return txCtx, nil
}

// Append adds a set of statements to a transaction using its transaction identifier (recovered from context.Context).
// Moreover, if transaction has no entries, this function will pre-allocate a buffer of transactions using
// MaxTransactionStatements as buffer's capacity.
//
// Finally, if a transaction or the given set of statements have a length greater than MaxTransactionStatements,
// items with index further than MaxTransactionStatements will be ignored.
//
// Note: Append is thread-safe.
func Append(ctx context.Context, stmt ...Statement) error {
	if internalRegistry == nil {
		internalRegistry = &sync.Map{}
	}
	if len(stmt) == 0 {
		return nil
	}

	id, err := GetID(ctx)
	if err != nil {
		return err
	}

	if len(stmt) >= MaxTransactionStatements {
		stmt = stmt[:MaxTransactionStatements-1] // ignore extra items
	}

	v, _ := internalRegistry.Load(id)
	buf := parseTxStatements(v)
	if buf == nil {
		buf = make([]Statement, 0, MaxTransactionStatements)
	}
	if len(buf) >= MaxTransactionStatements {
		buf = buf[:MaxTransactionStatements-1] // ignore extra items
	} else {
		// moving this to an else statement to avoid extra-allocation
		buf = append(buf, stmt...)
	}
	internalRegistry.Store(id, buf)
	return nil
}

// Get retrieves a set of statements using its transaction identifier (recovered from context.Context).
//
// Note: Get is thread-safe.
func Get(ctx context.Context) ([]Statement, error) {
	if internalRegistry == nil {
		return nil, nil
	}

	id, err := GetID(ctx)
	if err != nil {
		return nil, err
	}

	v, _ := internalRegistry.Load(id)
	return parseTxStatements(v), nil
}
