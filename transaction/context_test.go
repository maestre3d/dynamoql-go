package transaction_test

import (
	"context"
	"testing"

	"github.com/maestre3d/dynamoql/transaction"
)

func TestAppend(t *testing.T) {
	transaction.RegisterNoop()
	rootCtx := context.Background()

	ctx := transaction.NewContext(rootCtx, transaction.NoopDriverKey)
	err := transaction.Append(ctx, transaction.Statement{
		Kind:  transaction.ReadKind,
		Table: "GraphTable",
	}, transaction.Statement{
		Kind:  transaction.InsertKind,
		Table: "GraphTable",
	}, transaction.Statement{
		Kind:  transaction.UpdateKind,
		Table: "GraphTable",
	}, transaction.Statement{
		Kind:  transaction.DeleteKind,
		Table: "GraphTable",
	})
	_ = transaction.Append(ctx, transaction.Statement{
		Kind:  transaction.UpsertKind,
		Table: "GraphTable",
	})
	t.Log(err)
	id, _ := transaction.GetID(ctx)
	t.Log(id)

	out, err := transaction.Get(ctx)
	t.Log(err)
	t.Logf("%+v", out)
}

func BenchmarkAppend(b *testing.B) {
	transaction.RegisterNoop()
	rootCtx := context.Background()

	ctx := transaction.NewContext(rootCtx, transaction.NoopDriverKey)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = transaction.Append(ctx, transaction.Statement{
			Kind:  transaction.UpsertKind,
			Table: "GraphTable",
		})
	}
}
