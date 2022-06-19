package transaction

import (
	"context"
	"database/sql"
)

// SqlStatement SQL-specific transaction statement.
type SqlStatement struct {
	Query       string
	Args        [][]interface{}
	OnFetchFunc func(ctx context.Context, rows *sql.Rows) error
}

// SqlDriver driver for SQL databases.
type SqlDriver struct {
	db        *sql.DB
	isolation sql.IsolationLevel
}

var _ Driver = &SqlDriver{}

// RegisterSQL sets a SqlDriver into transaction's driver list using DynamoDBDriverName as key.
func RegisterSQL(db *sql.DB, isolation sql.IsolationLevel) {
	RegisterDriver(SQLDriverKey, &SqlDriver{db: db, isolation: isolation})
}

func marshalSqlStatements(stmts []Statement) ([]SqlStatement, error) {
	buf := make([]SqlStatement, len(stmts))
	for _, stmt := range stmts {
		item, ok := stmt.Operation.(SqlStatement)
		if !ok {
			return nil, ErrInvalidOperationType
		}
		buf = append(buf, item)
	}
	return buf, nil
}

func execPreparedStatement(ctx context.Context, tx *sql.Tx, query string, args []interface{}) error {
	sqlStmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			err = sqlStmt.Close()
		}
	}()
	_, err = sqlStmt.ExecContext(ctx, args...)
	return err
}

func (s *SqlDriver) Exec(ctx context.Context, statements []Statement) error {
	sqlStmts, err := marshalSqlStatements(statements)
	if err != nil {
		return err
	}

	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = conn.Close()
	}()

	tx, err := conn.BeginTx(ctx, &sql.TxOptions{
		Isolation: s.isolation,
		ReadOnly:  false,
	})
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
			return
		}
		_ = tx.Commit()
	}()
	for i, stmt := range sqlStmts {
		if statements[i].Kind == ReadKind {
			if len(stmt.Args) == 0 || stmt.OnFetchFunc == nil {
				return ErrInvalidOperationType
			}
			rows, errQuery := tx.QueryContext(ctx, stmt.Query, stmt.Args[0]...)
			if errQuery != nil {
				return errQuery
			} else if err = stmt.OnFetchFunc(ctx, rows); err != nil {
				return err
			}
			continue
		}
		if len(stmt.Args) == 1 {
			_, err = tx.ExecContext(ctx, stmt.Query, stmt.Args[0]...)
			if err != nil {
				return err
			}
			continue
		}
		for _, args := range stmt.Args {
			if err = execPreparedStatement(ctx, tx, stmt.Query, args); err != nil {
				return err
			}
		}
	}
	return nil
}
