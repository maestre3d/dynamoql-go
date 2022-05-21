package dynamoql

import "unsafe"

// NOTE IF strings.Builder usage is required.
//
// To avoid this issue using strings.Builder:
//
// strings.Builder internals uses the following formula: 2(len(buffer)) + n
//
// Hence it is required to revert this formula to allocate the exact required extra capacity of the buffer.
// growFactor := requiredCap - (2 * buf.Cap()) + buf.Len()
//
// Nevertheless, this workaround does not always work.

// stringBuffer efficiently write a string or byte.
//
// It differs from strings.Builder and bytes.Buffer as this implementation allocates the
// capacity of the internal buffer required for each write, allocating only when needed and with exact precision.
// Therefore, no buffer capacity duplication is required.
//
// Exposes methods to comply with strings.Builder and bytes.Buffer APIs.
//
// DO NOT copy after zero-value allocation.
type stringBuffer struct {
	buf []byte
}

func (b *stringBuffer) Cap() int {
	return cap(b.buf)
}

func (b *stringBuffer) Len() int {
	return len(b.buf)
}

func (b *stringBuffer) Grow(requiredCap int) {
	if requiredCap <= 0 {
		return
	}
	spaceAvail := cap(b.buf) - len(b.buf)
	if spaceAvail >= requiredCap {
		return
	}
	growFactor := requiredCap - spaceAvail
	if growFactor < 0 {
		growFactor = growFactor * -1
	}
	newBuf := make([]byte, len(b.buf), cap(b.buf)+growFactor)
	copy(newBuf, b.buf)
	b.buf = newBuf
}

func (b *stringBuffer) WriteByte(v byte) {
	b.Grow(1)
	b.buf = append(b.buf, v)
}

func (b *stringBuffer) WriteString(s string) {
	b.Grow(len(s))
	b.buf = append(b.buf, s...)
}

func (b *stringBuffer) String() string {
	return *(*string)(unsafe.Pointer(&b.buf))
}
