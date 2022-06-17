package transaction_test

import (
	"context"

	"github.com/maestre3d/dynamoql/transaction"
)

type driverMock struct{}

var _ transaction.Driver = driverMock{}

func (d driverMock) Commit(_ context.Context, _ []transaction.Statement) error {
	return nil
}

func (d driverMock) Rollback(_ context.Context, _ []transaction.Statement) error {
	return nil
}
