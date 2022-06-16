package transaction

// Statement instruction to be executed within a transaction by a database system.
type Statement struct {
	Kind  Kind
	Table string
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
