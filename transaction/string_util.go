package transaction

func newSafeStringPtr(v string) *string {
	if v == "" {
		return nil
	}
	return &v
}
