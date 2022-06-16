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
	// MaxTransactionStatements the maximum amount of statements per-transaction.
	//
	// It is very important to set this variable correctly as internal `Transaction Context` components
	// use it to pre-allocate memory (slice of statements of each transaction), increasing overall performance.
	//
	// Default is Amazon DynamoDB's limit (25 statements per-transaction).
	MaxTransactionStatements = 25

	// internalRegistry Map which keeps track of transactions statements using transaction identifiers as key
	// and a slice of Statement as value.
	internalRegistry *sync.Map
)

// txContext a mapping of an id and its Driver. Driver is required to perform commit and rollback operations.
type txContext struct {
	id int
	// Using string instead Driver as this will decrease overall struct copies.
	driver string
}

// NewContext builds a context.Context with a transaction identifier and its driver from a parent context.
func NewContext(ctx context.Context, driver string) context.Context {
	return context.WithValue(ctx, ContextKey, txContext{
		id:     rand.Int(),
		driver: driver,
	})
}

// GetID returns a transaction identifier from context.Context. Returns ErrMissingID if missing.
func GetID(ctx context.Context) (int, error) {
	txCtx, ok := ctx.Value(ContextKey).(txContext)
	if !ok {
		return 0, ErrMissingID
	}
	return txCtx.id, nil
}

// getContext returns a transaction context from context.Context.
func getContext(ctx context.Context) (txContext, error) {
	txCtx, ok := ctx.Value(ContextKey).(txContext)
	if !ok {
		return txContext{}, ErrMissingContext
	} else if _, ok = drivers[txCtx.driver]; !ok {
		return txContext{}, ErrMissingDriver
	}
	return txCtx, nil
}

// Append adds a set of statements to a transaction using its transaction identifier (recovered from context.Context).
// Moreover, if transaction has no entries, this function will pre-allocate a buffer of transactions using
// MaxTransactionStatements as buffer's capacity.
//
// Finally, if a transaction or the given set of statements have a length greater than MaxTransactionStatements,
// items with index further than MaxTransactionStatements will be ignored.
func Append(ctx context.Context, stmt ...Statement) error {
	if internalRegistry == nil {
		internalRegistry = &sync.Map{}
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

// Commit proceeds with the execution of the set of Statement from a transaction context.
func Commit(ctx context.Context) error {
	if internalRegistry == nil {
		return ErrRegistryNotStarted
	}

	txCtx, err := getContext(ctx)
	if err != nil {
		return err
	}

	v, _ := internalRegistry.LoadAndDelete(txCtx.id)
	buf := parseTxStatements(v)
	if buf == nil {
		return ErrMissingTransaction
	}
	return drivers[txCtx.driver].Commit(ctx, buf)
}

// Rollback cancels the execution of the set of Statement from a transaction context.
func Rollback(ctx context.Context) error {
	if internalRegistry == nil {
		return ErrRegistryNotStarted
	}

	txCtx, err := getContext(ctx)
	if err != nil {
		return err
	}

	v, _ := internalRegistry.LoadAndDelete(txCtx.id)
	buf := parseTxStatements(v)
	if buf == nil {
		return ErrMissingTransaction
	}
	return drivers[txCtx.driver].Rollback(ctx, buf)
}
