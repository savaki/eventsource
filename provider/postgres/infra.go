package postgres

import (
	"context"
	"database/sql"

	"github.com/savaki/eventsource/provider/sqlstore"
)

const (
	postgresCreateTable = `
	CREATE TABLE IF NOT EXISTS {{ .TableName }} (
	    "offset"  BIGSERIAL PRIMARY KEY NOT NULL,
	    id        VARCHAR(255) NOT NULL,
	    version   INTEGER NOT NULL,
	    data      JSON NOT NULL,
	    at        BIGINT NOT NULL
	)
`

	postgresUniqueIndex = `CREATE UNIQUE INDEX IF NOT EXISTS idx_{{ .TableName }} ON {{ .TableName }} (id, version)`
)

func CreatePostgres(ctx context.Context, db *sql.DB, tableName string) error {
	createSQL := sqlstore.ReTableName.ReplaceAllString(postgresCreateTable, tableName)

	_, err := db.ExecContext(ctx, createSQL)
	if err != nil {
		return err
	}

	indexes := []string{
		sqlstore.ReTableName.ReplaceAllString(postgresUniqueIndex, tableName),
	}

	for _, createIndexSQL := range indexes {
		_, err := db.ExecContext(ctx, createIndexSQL)
		if err != nil {
			return err
		}
	}

	return nil
}
