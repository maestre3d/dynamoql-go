package transaction

// Kind transaction statement's type of operation to be executed (e.g. SELECT, INSERT, UPSERT, UPDATE or DELETE).
//
// This type represents an enum.
type Kind int

const (
	// avoid zero-value (default) for transaction Kind enum.
	// This avoids critical mistakes where no value was given.
	_ Kind = iota
	// ReadKind loads an item (a.k.a. SELECT).
	ReadKind
	// InsertKind store an item (a.k.a. INSERT).
	InsertKind
	// UpdateKind physically update an existing item (a.k.a. UPDATE).
	UpdateKind
	// DeleteKind physically removes an existing item (a.k.a. DELETE).
	DeleteKind
	// UpsertKind create an item if not exists or update an item if otherwise (a.k.a. UPSERT, INSERT or UPDATE).
	UpsertKind
)
