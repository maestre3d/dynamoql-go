package dynamoql

import "strings"

// CompositeKeySeparator a pattern to subdivide a key name and its value.
var CompositeKeySeparator = "#"

// NewCompositeKey creates a key composed by the key itself, a separator (CompositeKeySeparator) and the actual key value;
// ready to be used by graph database implementations (e.g. Amazon DynamoDB with adjacency list graph pattern).
//
// The nomenclature is the following: {PREFIX}{SEPARATOR}{VALUE}.
//
// e.g. USER#123
func NewCompositeKey(prefix, v string) string {
	if prefix == "" && v == "" {
		return ""
	}

	buf := strings.Builder{}
	growFactor := len(prefix) + len(v) + len(CompositeKeySeparator)
	buf.Grow(growFactor)
	buf.WriteString(prefix)
	buf.WriteString(CompositeKeySeparator)
	buf.WriteString(v)
	return buf.String()
}

// ParseCompositeKey removes prefix and separator (CompositeKeySeparator). If no separator found, returns k.
func ParseCompositeKey(k string) string {
	spl := strings.SplitN(k, CompositeKeySeparator, 2)
	switch len(spl) {
	case 1:
		return spl[0]
	case 2:
		return spl[1]
	}
	return ""
}

// NewOverloadedKey creates a key composed by N values and a separator between values.
// This key crafting technique is used for advanced search queries using the sort key with BeginsWith or Between
// operators mostly.
//
// The nomenclature is the following: {VALUE_0}{SEPARATOR}{VALUE_N}.
//
// e.g. London:Westminster:Downing St:10
func NewOverloadedKey(sep string, v ...string) string {
	// This validation is done to avoid arbitrary (and thus invalid) overload keys.
	if len(v) == 0 || sep == "" {
		return ""
	}
	growFactor := len(sep) * (len(v) - 1)
	for i := range v {
		growFactor += len(v[i])
	}
	buf := strings.Builder{}
	buf.Grow(growFactor)
	for i := range v {
		if i > 0 {
			buf.WriteString(sep)
		}
		buf.WriteString(v[i])
	}
	return buf.String()
}
