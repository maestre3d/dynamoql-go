package dynamodb_go

import (
	"bytes"
	"encoding/base64"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	pageTokenKeySeparator  = '&'
	pageTokenAttrSeparator = '='
	pageTokenTypeSeparator = '|'
)

// PageToken is a DynamoDB Last Evaluate Key(s) from Query and Scan APIs. This is a base64-based custom type used
// to represent the Last Evaluate Key(s) as URL-safe string to be used by clients (if developing a HTTP/gRPC/... API).
//
// Note: Last Evaluate Key(s) is the primary key of a DynamoDB table. Primary keys accept String, Binary and Number
// DynamoDB types and have a maximum length of 2 keys (Partition Key and Sort Key, which compose a composite key
// if both present).
//
// See ref: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.CoreComponents.html
type PageToken map[string]types.AttributeValue

func (t PageToken) toBinary() []byte {
	buffer := new(bytes.Buffer)
	// Note: Might require to use a LinkedHashMap to preserve insertion-order.
	count := 0
	for k := range t {
		if count > 1 {
			// ignore extra keys, a primary is composed by a Partition Key and a Sort key only
			break
		}
		switch t[k].(type) {
		case *types.AttributeValueMemberS:
			attr := t[k].(*types.AttributeValueMemberS)
			buffer.WriteByte('S')
			buffer.WriteByte(pageTokenTypeSeparator)
			_, _ = buffer.WriteString(k)
			buffer.WriteByte(pageTokenAttrSeparator)
			_, _ = buffer.WriteString(attr.Value)
		case *types.AttributeValueMemberN:
			attr := t[k].(*types.AttributeValueMemberN)
			buffer.WriteByte('N')
			buffer.WriteByte(pageTokenTypeSeparator)
			_, _ = buffer.WriteString(k)
			buffer.WriteByte(pageTokenAttrSeparator)
			_, _ = buffer.WriteString(attr.Value)
		case *types.AttributeValueMemberB:
			attr := t[k].(*types.AttributeValueMemberB)
			buffer.WriteByte('B')
			buffer.WriteByte(pageTokenTypeSeparator)
			_, _ = buffer.WriteString(k)
			buffer.WriteByte(pageTokenAttrSeparator)
			buffer.Write(attr.Value)
		}
		if count < len(t)-1 {
			buffer.WriteByte(pageTokenKeySeparator)
		}
		count++
	}
	return buffer.Bytes()
}

func (t PageToken) fromBinary(raw []byte) error {
	keys := strings.SplitN(string(raw), string(pageTokenKeySeparator), 2)
	for i := range keys {
		kv := strings.SplitN(keys[i], string(pageTokenAttrSeparator), 2)
		if len(kv) != 2 {
			continue
		}
		typeAndKey := strings.SplitN(kv[0], string(pageTokenTypeSeparator), 2)
		if len(typeAndKey) != 2 {
			continue
		}
		switch typeAndKey[0] {
		case "S":
			t[typeAndKey[1]] = &types.AttributeValueMemberS{
				Value: kv[1],
			}
		case "N":
			t[typeAndKey[1]] = &types.AttributeValueMemberN{
				Value: kv[1],
			}
		case "B":
			t[typeAndKey[1]] = &types.AttributeValueMemberB{
				Value: []byte(kv[1]),
			}
		}
	}
	return nil
}

// Encode transforms the current PageToken into a base64 URL-safe string.
func (t PageToken) Encode() string {
	return base64.URLEncoding.EncodeToString(t.toBinary())
}

// String transforms the current PageToken into a base64 URL-safe string.
//
// Wraps PageToken.Encode().
func (t PageToken) String() string {
	return t.Encode()
}

// Decode converts given base64 URL-safe string into a PageToken.
func (t PageToken) Decode(encodedRaw string) error {
	data, err := base64.URLEncoding.DecodeString(encodedRaw)
	if err != nil {
		return err
	}
	return t.fromBinary(data)
}
