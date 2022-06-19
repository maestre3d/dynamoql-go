package transaction_test

import (
	"context"
	"testing"

	"github.com/maestre3d/dynamoql/transaction"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetID(t *testing.T) {
	tests := []struct {
		name   string
		in     context.Context
		wantId bool
		err    error
	}{
		{
			name:   "Nil",
			in:     nil,
			wantId: false,
			err:    transaction.ErrMissingContext,
		},
		{
			name:   "No tx context",
			in:     context.Background(),
			wantId: false,
			err:    transaction.ErrMissingContext,
		},
		{
			name:   "Valid",
			in:     transaction.NewContext(context.Background()),
			wantId: true,
			err:    nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exp, err := transaction.GetID(tt.in)
			assert.Equal(t, tt.err, err)
			assert.Equal(t, tt.wantId, exp != 0)
		})
	}
}

func BenchmarkGetID(b *testing.B) {
	ctx := transaction.NewContext(context.TODO())
	seed := []transaction.Statement{
		{
			Kind:  transaction.ReadKind,
			Table: "GraphTable",
		}, {
			Kind:  transaction.InsertKind,
			Table: "GraphTable",
		},
	}
	if err := transaction.Append(ctx, seed...); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = transaction.GetID(ctx)
	}
}

func TestAppend(t *testing.T) {
	rootCtx := context.Background()
	transaction.MaxTransactionStatements = 5
	tests := []struct {
		name     string
		in       []transaction.Statement
		seedFunc func(t *testing.T, ctx context.Context)
		err      error
		expLen   int
	}{
		{
			name:   "Empty",
			in:     nil,
			err:    nil,
			expLen: 0,
		},
		{
			name: "Multi value out of range and populated internal buffer",
			in: []transaction.Statement{
				{
					Kind:  transaction.ReadKind,
					Table: "GraphTable",
				}, {
					Kind:  transaction.InsertKind,
					Table: "GraphTable",
				}, {
					Kind:  transaction.UpdateKind,
					Table: "GraphTable",
				}, {
					Kind:  transaction.DeleteKind,
					Table: "GraphTableLastReachable",
				}, {
					Kind:  transaction.DeleteKind,
					Table: "GraphTableNotReachable",
				}, {
					Kind:  transaction.ReadKind,
					Table: "GraphTableNotReachable",
				},
			},
			seedFunc: func(t *testing.T, ctx context.Context) {
				err := transaction.Append(ctx, transaction.Statement{
					Kind:  transaction.ReadKind,
					Table: "GraphTableSeed",
				})
				require.NoError(t, err)
			},
			err:    nil,
			expLen: 5,
		},
		{
			name: "Multi value out of range",
			in: []transaction.Statement{
				{
					Kind:  transaction.ReadKind,
					Table: "GraphTable",
				}, {
					Kind:  transaction.InsertKind,
					Table: "GraphTable",
				}, {
					Kind:  transaction.UpdateKind,
					Table: "GraphTable",
				}, {
					Kind:  transaction.DeleteKind,
					Table: "GraphTable",
				}, {
					Kind:  transaction.DeleteKind,
					Table: "GraphTableLastReachable",
				}, {
					Kind:  transaction.ReadKind,
					Table: "GraphTableNotReachable",
				},
			},
			err:    nil,
			expLen: 5,
		},
		{
			name: "Multi value",
			in: []transaction.Statement{
				{
					Kind:  transaction.ReadKind,
					Table: "GraphTable",
				}, {
					Kind:  transaction.InsertKind,
					Table: "GraphTable",
				}, {
					Kind:  transaction.UpdateKind,
					Table: "GraphTable",
				}, {
					Kind:  transaction.DeleteKind,
					Table: "GraphTable",
				},
			},
			err:    nil,
			expLen: 4,
		},
		{
			name: "Single value",
			in: []transaction.Statement{
				{
					Kind:  transaction.UpsertKind,
					Table: "GraphTable",
				},
			},
			err:    nil,
			expLen: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := transaction.NewContext(rootCtx)
			if tt.seedFunc != nil {
				tt.seedFunc(t, ctx)
			}
			err := transaction.Append(ctx, tt.in...)
			assert.Equal(t, tt.err, err)
			out, _ := transaction.Get(ctx)
			assert.Len(t, out, tt.expLen)
		})
	}
}

func BenchmarkAppend(b *testing.B) {
	rootCtx := context.Background()
	ctx := transaction.NewContext(rootCtx)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = transaction.Append(ctx, transaction.Statement{
			Kind:  transaction.UpsertKind,
			Table: "GraphTable",
		})
	}
}

func TestGet(t *testing.T) {
	// seed data for valid ops
	txContext := transaction.NewContext(context.TODO())
	seed := []transaction.Statement{
		{
			Kind:  transaction.ReadKind,
			Table: "GraphTable",
		}, {
			Kind:  transaction.InsertKind,
			Table: "GraphTable",
		},
	}
	errSeed := transaction.Append(txContext, seed...)
	require.NoError(t, errSeed)

	tests := []struct {
		name   string
		ctx    context.Context
		expLen int
		err    error
	}{
		{
			name:   "Nil",
			ctx:    nil,
			expLen: 0,
			err:    transaction.ErrMissingContext,
		},
		{
			name:   "Empty ctx",
			ctx:    context.TODO(),
			expLen: 0,
			err:    transaction.ErrMissingContext,
		},
		{
			name:   "Valid",
			ctx:    txContext,
			expLen: len(seed),
			err:    nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := transaction.Get(tt.ctx)
			assert.Equal(t, tt.err, err)
			assert.Len(t, out, tt.expLen)
		})
	}
}

func BenchmarkGet(b *testing.B) {
	ctx := transaction.NewContext(context.TODO())
	seed := []transaction.Statement{
		{
			Kind:  transaction.ReadKind,
			Table: "GraphTable",
		}, {
			Kind:  transaction.InsertKind,
			Table: "GraphTable",
		},
	}
	if err := transaction.Append(ctx, seed...); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = transaction.Get(ctx)
	}
}

func TestNewContext(t *testing.T) {
	tests := []struct {
		name      string
		in        context.Context
		expDriver string
		err       error
	}{
		{
			name:      "Nil",
			in:        nil,
			expDriver: "",
			err:       transaction.ErrMissingContext,
		},
		{
			name:      "Non-nil context",
			in:        context.TODO(),
			expDriver: "noop", // default driver
			err:       nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := transaction.NewContext(tt.in)
			out, err := transaction.GetContext(ctx)
			assert.Equal(t, tt.err, err)
			if err != nil {
				return
			}
			assert.Equal(t, tt.expDriver, out.Driver)
			assert.Greater(t, out.ID, 0)
		})
	}
}

func TestNewContextWithDriver(t *testing.T) {
	transaction.RegisterDriver("mock", driverMock{})
	tests := []struct {
		name      string
		in        context.Context
		inDriver  string
		expDriver string
		err       error
	}{
		{
			name:      "Nil",
			in:        nil,
			inDriver:  "",
			expDriver: "",
			err:       transaction.ErrMissingContext,
		},
		{
			name:      "No driver specified",
			in:        context.TODO(),
			inDriver:  "",
			expDriver: "",
			err:       transaction.ErrMissingDriver,
		},
		{
			name:      "Unknown driver specified",
			in:        context.TODO(),
			inDriver:  "foo",
			expDriver: "",
			err:       transaction.ErrMissingDriver,
		},
		{
			name:      "Existing driver",
			in:        context.TODO(),
			inDriver:  "mock",
			expDriver: "mock",
			err:       nil,
		},
		{
			name:      "Existing driver and double tx context", // will replace driver
			in:        transaction.NewContext(context.TODO()),
			inDriver:  "mock",
			expDriver: "mock",
			err:       nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := transaction.NewContextWithDriver(tt.in, tt.inDriver)
			out, err := transaction.GetContext(ctx)
			assert.Equal(t, tt.err, err)
			if err != nil {
				return
			}
			assert.Equal(t, tt.expDriver, out.Driver)
			assert.Greater(t, out.ID, 0)
		})
	}
}
