package mysql

import (
	"context"
	"database/sql"

	"github.com/go-sql-driver/mysql"
	"github.com/savaki/eventsource/provider/sqlstore"
)

const (
	mysqlCreateTable = `
	CREATE TABLE IF NOT EXISTS {{ .TableName }} (
	    offset    BIGINT(20) PRIMARY KEY NOT NULL AUTO_INCREMENT,
	    id        VARCHAR(255),
	    version   INT,
	    data      VARBINARY(8192),
	    at        BIGINT(20)
	) CHARACTER SET utf8 COLLATE utf8_unicode_ci AUTO_INCREMENT=10000;
`

	mysqlUniqueIndex = `CREATE UNIQUE INDEX idx_{{ .TableName }} ON {{ .TableName }} (id, version)`
)

func CreateMySQL(ctx context.Context, db *sql.DB, tableName string) error {
	createSQL := sqlstore.ReTableName.ReplaceAllString(mysqlCreateTable, tableName)

	_, err := db.ExecContext(ctx, createSQL)
	if err != nil {
		return err
	}

	indexes := []string{
		sqlstore.ReTableName.ReplaceAllString(mysqlUniqueIndex, tableName),
	}

	for _, createIndexSQL := range indexes {
		_, err := db.ExecContext(ctx, createIndexSQL)
		if err != nil {
			if v, ok := err.(*mysql.MySQLError); ok {
				if v.Number == 0x425 {
					continue
				}
			}
			return err
		}
	}

	return nil
}
