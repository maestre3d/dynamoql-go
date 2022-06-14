package dynamoql

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// ErrReaderEOF reader has reached the end of the file.
var ErrReaderEOF = errors.New("dynamoql: Reader has reached end of file")

// QueryReader iterates for each item stored in an Amazon DynamoDB table using the Query API.
//
// Uses a pre-fetching strategy, loading chunks of data (specified by the user, default is 10 items per-chunk) into
// an internal buffer before actual item iteration.
//
// When a reader reaches last item from the local data chunk, it will automatically fetch the next chunk until
// no more chunks are left (if iteration is not stopped in the meanwhile).
//
// Some example for using QueryReader:
//
//	bills := make([]Bill, 0, 10)
//	for r.Next() {
//		item, err := r.GetItem(ctx)
//		if err != nil {
//			break
//		}
//
//		bill := Bill{}
//		if err = bill.UnmarshalDynamoDB(item); err != nil {
//			break
//		}
//
//		bills = append(bills, bill)
//		if r.Count() >= 10 {
//			break
//		}
//	}
type QueryReader struct {
	paginator *QueryPaginator
	buf       *ItemBuffer
	hasNext   bool
	readPivot int
	itemCount int
}

// NewQueryReader allocates a QueryReader with required internal components. Returns nil if a nil
// dynamodb.QueryInput is passed.
func NewQueryReader(chunkSize int32, c *dynamodb.Client, q *dynamodb.QueryInput) *QueryReader {
	if q == nil {
		return nil
	}
	return &QueryReader{
		paginator: NewQueryPaginator(chunkSize, c, q),
		buf:       NewItemBuffer(int(chunkSize)),
		readPivot: 0,
		hasNext:   true,
	}
}

// Next indicates if there is another item to get.
func (q *QueryReader) Next() bool {
	return q.hasNext
}

// Loads chunks of data into the buffer.
//
// Uses a QueryPaginator as underlying item fetching mechanism.
func (q *QueryReader) read(ctx context.Context) error {
	if q.buf.IsFull() {
		q.buf.Reset()
		q.readPivot = 0
	}
	for q.paginator.Next() {
		out, err := q.paginator.GetPage(ctx)
		if err != nil {
			return err
		} else if len(out.Items) == 0 {
			return ErrReaderEOF
		}
		q.buf.WriteItems(out.Items)
		if int(q.paginator.Count()) >= q.buf.Cap() {
			break
		}
	}
	return nil
}

// Count returns the count of each item retrieved by a QueryReader instance.
func (q *QueryReader) Count() int {
	return q.itemCount
}

// GetItem retrieves an Item from an Amazon DynamoDB table.
func (q *QueryReader) GetItem(ctx context.Context) (map[string]types.AttributeValue, error) {
	if q.buf.Len() == 0 || q.readPivot > q.buf.Len()-1 {
		if err := q.read(ctx); err != nil {
			return nil, err
		}
	}
	item := q.buf.ItemAt(q.readPivot)
	q.readPivot++
	q.itemCount++
	q.hasNext = q.buf.PeekAt(q.readPivot) || q.paginator.lastEvalKey != nil
	return item, nil
}
