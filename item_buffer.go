package dynamoql

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// ItemBuffer efficiently hold and interact with collections of Amazon DynamoDB items.
type ItemBuffer struct {
	buf []map[string]types.AttributeValue
}

// NewItemBuffer allocates a new ItemBuffer with the given size.
func NewItemBuffer(size int) *ItemBuffer {
	return &ItemBuffer{
		buf: make([]map[string]types.AttributeValue, 0, size),
	}
}

// Cap retrieves internal buffer capacity.
func (b *ItemBuffer) Cap() int {
	return cap(b.buf)
}

// Len retrieves internal buffer length.
func (b *ItemBuffer) Len() int {
	return len(b.buf)
}

// IsFull checks if the internal buffer is full.
func (b *ItemBuffer) IsFull() bool {
	return len(b.buf)-cap(b.buf) == 0
}

// Grow increases the memory allocation (malloc) of the internal buffer.
func (b *ItemBuffer) Grow(n int) {
	growFactor := len(b.buf)*2 + n
	newBuf := make([]map[string]types.AttributeValue, b.Len(), growFactor)
	copy(newBuf, b.buf)
	b.buf = nil
	b.buf = newBuf
}

// Reset removes all items from the internal buffer, retaining the original capacity.
func (b *ItemBuffer) Reset() {
	b.buf = b.buf[:0]
}

// Write stores the given value into the buffer.
func (b *ItemBuffer) Write(v map[string]types.AttributeValue) {
	b.buf = append(b.buf, v)
}

// WriteItems stores the given set of values into the buffer.
func (b *ItemBuffer) WriteItems(v []map[string]types.AttributeValue) {
	b.buf = append(b.buf, v...)
}

// Items retrieves the current collection of items.
func (b *ItemBuffer) Items() []map[string]types.AttributeValue {
	return b.buf
}

// ItemAt retrieves a specific item using its position.
func (b *ItemBuffer) ItemAt(n int) map[string]types.AttributeValue {
	if n >= len(b.buf) {
		return nil
	}
	return b.buf[n]
}

// PeekAt checks if an item is present on the given position.
func (b *ItemBuffer) PeekAt(n int) bool {
	if n >= len(b.buf) {
		return false
	}
	return b.buf[n] != nil
}
