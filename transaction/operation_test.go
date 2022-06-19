package transaction_test

import (
	"context"
	"testing"

	"github.com/maestre3d/dynamoql/transaction"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCommitAndRollback(t *testing.T) {
	const (
		_ uint8 = iota // commit var
		opRollback
	)

	// cover case when internal registry hasn't been started
	err := transaction.Commit(context.TODO())
	if err != nil {
		// start registry
		err = transaction.Append(context.TODO())
		require.NoError(t, err)
	}
	// require.ErrorIs(t, err, transaction.ErrRegistryNotStarted)

	tests := []struct {
		name     string
		in       context.Context
		seedFunc func(t *testing.T, ctx context.Context)
		op       uint8
		err      error
	}{
		{
			name: "Nil ctx",
			in:   nil,
			err:  transaction.ErrMissingContext,
		},
		{
			name: "Invalid tx ctx",
			in:   context.TODO(),
			err:  transaction.ErrMissingContext,
		},
		{
			name: "empty tx context",
			in:   transaction.NewContext(context.TODO()),
			err:  transaction.ErrMissingTransaction,
		},
		{
			name: "populated tx context commit",
			in:   transaction.NewContext(context.TODO()),
			seedFunc: func(t *testing.T, ctx context.Context) {
				err = transaction.Append(ctx, transaction.Statement{
					Kind:  transaction.ReadKind,
					Table: "foo",
				})
				require.NoError(t, err)
			},
			err: nil,
		},
		{
			name: "populated tx context rollback",
			in:   transaction.NewContext(context.TODO()),
			seedFunc: func(t *testing.T, ctx context.Context) {
				err = transaction.Append(ctx, transaction.Statement{
					Kind:  transaction.ReadKind,
					Table: "foo",
				})
				require.NoError(t, err)
			},
			op:  opRollback,
			err: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.seedFunc != nil {
				tt.seedFunc(t, tt.in)
			}

			if tt.op == opRollback {
				err = transaction.Rollback(tt.in)
			} else {
				err = transaction.Commit(tt.in)
			}
			assert.Equal(t, tt.err, err)
		})
	}
}
