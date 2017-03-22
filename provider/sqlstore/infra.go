package sqlstore

import (
	"context"
	"database/sql"
	"regexp"

	"github.com/go-sql-driver/mysql"
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

var (
	reTableName = regexp.MustCompile(`\{\{\s*.TableName\s*}}`)
)

func CreateMySQL(ctx context.Context, db *sql.DB, tableName string) error {
	createSQL := reTableName.ReplaceAllString(mysqlCreateTable, tableName)

	_, err := db.ExecContext(ctx, createSQL)
	if err != nil {
		return err
	}

	indexes := []string{
		reTableName.ReplaceAllString(mysqlUniqueIndex, tableName),
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
