package transaction

import "errors"

var (
	// ErrMissingContext the transaction context was missing from scoped context.Context.
	ErrMissingContext = errors.New("dynamoql: Missing transaction context from context")
	// ErrMissingTransaction the transaction was missing from scoped context.Context.
	ErrMissingTransaction = errors.New("dynamoql: Missing transaction from context")
	// ErrRegistryNotStarted the transaction context registry has not started yet.
	ErrRegistryNotStarted = errors.New("dynamoql: Transaction registry has not been started")
	// ErrMissingDriver the transaction context has no driver registered.
	ErrMissingDriver = errors.New("dynamoql: Missing driver for transaction context")
	// ErrUnknownOperationKind the given Kind is not known.
	ErrUnknownOperationKind = errors.New("dynamoql: Unknown transaction operation kind")
	// ErrInvalidOperationType the given Statement.Operation has an invalid format.
	ErrInvalidOperationType = errors.New("dynamoql: Invalid transaction operation type")
)
