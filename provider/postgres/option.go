package postgres

import (
	"fmt"
	"strings"

	"github.com/savaki/eventsource/provider/sqlstore"
)

func WithDialectPostgres() sqlstore.Option {
	return func(s *sqlstore.Store) {
		s.InsertSQL = sqlToDollar(s.InsertSQL)
		s.SelectSQL = sqlToDollar(s.SelectSQL)
		s.SelectVersionSQL = sqlToDollar(s.SelectVersionSQL)
		s.SelectAllSQL = sqlToDollar(s.SelectAllSQL)
	}
}

func sqlToDollar(sql string) string {
	count := strings.Count(sql, "?")
	for i := 0; i < count; i++ {
		sql = strings.Replace(sql, "?", fmt.Sprintf("$%d", i+1), 1)
	}
	return sql
}
