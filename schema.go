package dynamoql

type Schema interface {
	Marshaler
	Unmarshaler
}
