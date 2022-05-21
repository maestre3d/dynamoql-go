package dynamoql

import (
	"bytes"
	"encoding/base64"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	pageTokenSeparator    = '~'
	pageTokenKeySeparator = '&'

	pageTokenAttrTypeString = 'S'
	pageTokenAttrTypeNumber = 'N'
	pageTokenAttrTypeBinary = 'B'
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

// NewPageTokenString converts the given base64-coded string into a PageToken.
//
// It will panic if fails to decode base64 string.
func NewPageTokenString(rawStr string) PageToken {
	t := PageToken{}
	if err := t.Decode(rawStr); err != nil {
		panic(err)
	}
	return t
}

// NewPageToken converts the given base64-coded string into a PageToken.
func NewPageToken(rawStr string) (PageToken, error) {
	t := PageToken{}
	if err := t.Decode(rawStr); err != nil {
		return nil, err
	}
	return t, nil
}

func (t PageToken) toBinary() []byte {
	// Page Token format:
	//
	// Partition Key only:
	//
	// Attr_Type~Attr_name~Attr_val
	//
	// Partition Key and Sort Key (Composite Key):
	//
	// Attr_Type~Attr_name~Attr_val&Attr_Type~Attr_name~Attr_val
	//
	// Or
	//
	// Key_0&Key_1
	buffer := bytes.NewBuffer(nil)
	// Note: Might require to use a LinkedHashMap to preserve insertion-order.
	count := 0
	for k := range t {
		if count > 1 {
			// ignore extra keys, a primary is composed by a Partition Key and a Sort key only
			break
		}
		switch t[k].(type) {
		case *types.AttributeValueMemberS:
			buffer.WriteByte(pageTokenAttrTypeString)
			buffer.WriteByte(pageTokenSeparator)
			_, _ = buffer.WriteString(k)
			buffer.WriteByte(pageTokenSeparator)
			attr := t[k].(*types.AttributeValueMemberS)
			_, _ = buffer.WriteString(attr.Value)
		case *types.AttributeValueMemberN:
			buffer.WriteByte(pageTokenAttrTypeNumber)
			buffer.WriteByte(pageTokenSeparator)
			_, _ = buffer.WriteString(k)
			buffer.WriteByte(pageTokenSeparator)
			attr := t[k].(*types.AttributeValueMemberN)
			_, _ = buffer.WriteString(attr.Value)
		case *types.AttributeValueMemberB:
			buffer.WriteByte(pageTokenAttrTypeBinary)
			buffer.WriteByte(pageTokenSeparator)
			_, _ = buffer.WriteString(k)
			buffer.WriteByte(pageTokenSeparator)
			attr := t[k].(*types.AttributeValueMemberB)
			buffer.Write(attr.Value)
		}
		if count < len(t)-1 {
			buffer.WriteByte(pageTokenKeySeparator)
		}
		count++
	}
	return buffer.Bytes()
}

func (t PageToken) append(attrType byte, key string, val []byte) {
	switch attrType {
	case pageTokenAttrTypeString:
		t[key] = &types.AttributeValueMemberS{
			Value: string(val),
		}
	case pageTokenAttrTypeNumber:
		t[key] = &types.AttributeValueMemberN{
			Value: string(val),
		}
	case pageTokenAttrTypeBinary:
		t[key] = &types.AttributeValueMemberB{
			Value: val,
		}
	}
}

func (t PageToken) fromBinary(raw []byte) error {
	// Page Token format:
	//
	// Partition Key only:
	//
	// Attr_Type~Attr_name~Attr_val
	//
	// Partition Key and Sort Key (Composite Key):
	//
	// Attr_Type~Attr_name~Attr_val&Attr_Type~Attr_name~Attr_val
	//
	// Or
	//
	// Key_0&Key_1
	if len(raw) < 2 {
		return nil
	}

	var attrType byte
	isType := true
	totalSep := 0
	queue := bytes.NewBuffer(nil)
	name := ""
	var val []byte
charLoop:
	for i := range raw {
		if len(t) == 2 {
			break
		}
		if isType {
			attrType = raw[i]
			isType = false
			continue
		}
		switch raw[i] {
		case pageTokenKeySeparator:
			val = queue.Bytes()
			isType = true
			queue.Reset()
			t.append(attrType, name, val)
			continue charLoop
		case pageTokenSeparator:
			totalSep++
			if totalSep%2 == 0 {
				name = queue.String()
				queue.Reset()
			}
			continue charLoop
		}
		queue.WriteByte(raw[i])
		if i == len(raw)-1 {
			val = queue.Bytes()
			queue.Reset()
			t.append(attrType, name, val)
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
