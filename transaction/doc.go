// Package transaction is a database-agnostic utility set of rich functionalities to perform transactions.
//
// In particular, it implements a `Transaction Context` which is a mechanism to aggregate a set of transaction
// statements from any part of the code using context.Context only; this enables code decoupling and, hence
// a cleaner architecture implementation.
package transaction
