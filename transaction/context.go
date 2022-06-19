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
func Append(ctx context.Context, stmts ...Statement) error {
	if internalRegistry == nil {
		internalRegistry = &sync.Map{}
	}
	if len(stmts) == 0 {
		return nil
	}

	id, err := GetID(ctx)
	if err != nil {
		return err
	}

	v, _ := internalRegistry.Load(id)
	buf := parseTxStatements(v)
	// Internal buffer is always initialized with cap = MaxTransactionStatements.
	//
	// Hence, to avoid new buffer malloc by go internals -append items' length > buffer's cap size-,
	// this function shrinks input slice to the remaining unassigned memory address blocks (capacity)
	// of the internal buffer.
	//
	// Thus, for a scenario where:
	//
	// len(stmts) = 6
	// MaxTransactionStatements = cap(buf) = 5
	// len(buf) = 1
	// remainingSpace = cap(buf) - len(buf) = 5 - 1 = 4
	//
	// Therefore, final stmts' length would be equals to 4.
	//
	// Important note: this function DOES NOT reduce stmts' capacity as this operation would require another malloc,
	// increasing memory footprint.
	if buf == nil {
		buf = make([]Statement, 0, MaxTransactionStatements)
	}
	remainingCap := cap(buf) - len(buf)
	if remainingCap < len(stmts) {
		stmts = stmts[:remainingCap]
	}
	buf = append(buf, stmts...)
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
