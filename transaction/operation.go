package transaction

import "context"

type operation uint8

const (
	_ operation = iota
	commitOperation
	rollbackOperation
)

// executeOperation performs either a Commit or Rollback operation based on the arguments.
func executeOperation(ctx context.Context, op operation) error {
	if internalRegistry == nil {
		return ErrRegistryNotStarted
	}
	txCtx, err := GetContext(ctx)
	if err != nil {
		return err
	}

	v, _ := internalRegistry.LoadAndDelete(txCtx.ID)
	buf := parseTxStatements(v)
	if buf == nil {
		return ErrMissingTransaction
	}
	if op == commitOperation {
		return drivers[txCtx.Driver].Commit(ctx, buf)
	}
	return drivers[txCtx.Driver].Rollback(ctx, buf)
}

// Commit proceeds with the execution of the set of Statement from a transaction context.
//
// Note: Commit is thread-safe.
func Commit(ctx context.Context) error {
	return executeOperation(ctx, commitOperation)
}

// Rollback cancels the execution of the set of Statement from a transaction context.
//
// Note: Rollback is thread-safe.
func Rollback(ctx context.Context) error {
	return executeOperation(ctx, rollbackOperation)
}
