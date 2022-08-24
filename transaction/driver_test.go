package transaction_test

import (
	"context"

	"github.com/maestre3d/dynamoql-go/transaction"
)

type driverMock struct{}

var _ transaction.Driver = driverMock{}

func (d driverMock) Exec(_ context.Context, _ []transaction.Statement) error {
	return nil
}
