package transaction

// Statement instruction to be executed within a transaction by a database system.
type Statement struct {
	// Kind of operation to be executed.
	Kind Kind
	// Operation to be executed by a Driver.
	Operation interface{}
}

// parseTxStatements converts a generic interface to a Statement slice.
func parseTxStatements(v interface{}) []Statement {
	if v == nil {
		return nil
	} else if out, ok := v.([]Statement); ok { // safe check, avoid panics
		return out
	}
	return nil
}
