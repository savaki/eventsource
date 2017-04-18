package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"regexp"
	"sort"
	"time"

	"github.com/savaki/eventsource"
)

const (
	sqlInsert        = `INSERT INTO {{ .TableName }} (id, version, data, at) VALUES (?, ?, ?, ?)`
	sqlSelectVersion = `SELECT version, data, at FROM {{ .TableName }} WHERE id = ? and version <= ?`
	sqlSelect        = `SELECT version, data, at FROM {{ .TableName }} WHERE id = ?`
	sqlSelectAll     = `SELECT version, data, at FROM {{ .TableName }}`
)

var (
	ReTableName = regexp.MustCompile(`\{\{\s*.TableName\s*}}`)
)

type Store struct {
	db               *sql.DB
	tableName        string
	InsertSQL        string
	SelectSQL        string
	SelectVersionSQL string
	SelectAllSQL     string
	debug            bool
	writer           io.Writer
}

func (s *Store) Save(ctx context.Context, aggregateID string, records ...eventsource.Record) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}

	s.log("Saving", len(records), "events.")

	err = func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(s.InsertSQL)
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, record := range records {
			s.log("Saving version,", record.Version)
			_, err = stmt.Exec(aggregateID, record.Version, record.Data, record.At)
			if err != nil {
				return err
			}
		}

		return nil
	}(tx)

	if err == nil {
		s.log("Ok")
		return tx.Commit()
	} else {
		s.log("Failed.  Rolling back transaction.")
		tx.Rollback()
		return err
	}
}

func (s *Store) Fetch(ctx context.Context, aggregateID string, version int) (eventsource.History, error) {
	s.log("Reading events with aggregrateID,", aggregateID)
	var rows *sql.Rows
	if aggregateID == "" {
		if rs, err := s.db.QueryContext(ctx, s.SelectAllSQL); err != nil {
			return eventsource.History{}, err
		} else {
			rows = rs
		}
	} else if version > 0 {
		if rs, err := s.db.QueryContext(ctx, s.SelectVersionSQL, aggregateID, version); err != nil {
			return eventsource.History{}, err
		} else {
			rows = rs
		}
	} else {
		if rs, err := s.db.QueryContext(ctx, s.SelectSQL, aggregateID); err != nil {
			return eventsource.History{}, err
		} else {
			rows = rs
		}
	}
	defer rows.Close()

	s.log("Scanning rows")
	history := make(eventsource.History, 0, version+1)
	for rows.Next() {
		s.log("Scanning row")
		version := 0
		data := []byte{}
		at := eventsource.EpochMillis(0)
		err := rows.Scan(&version, &data, &at)
		if err != nil {
			return eventsource.History{}, err
		}

		s.log("Reading version,", version)
		history = append(history, eventsource.Record{
			Version: version,
			At:      at,
			Data:    data,
		})
	}

	sort.Slice(history, func(i, j int) bool {
		return history[i].Version < history[j].Version
	})

	s.log("Successfully read", len(history), "events")
	return history, nil
}

func (s *Store) log(args ...interface{}) {
	if !s.debug {
		return
	}

	v := append([]interface{}{time.Now().Format(time.StampMilli)}, args...)
	fmt.Fprintln(s.writer, v...)
}

func New(tableName string, db *sql.DB, opts ...Option) *Store {
	insertSQL := ReTableName.ReplaceAllString(sqlInsert, tableName)
	selectSQL := ReTableName.ReplaceAllString(sqlSelect, tableName)
	selectVersionSQL := ReTableName.ReplaceAllString(sqlSelectVersion, tableName)
	selectAllSQL := ReTableName.ReplaceAllString(sqlSelectAll, tableName)

	s := &Store{
		db:               db,
		tableName:        tableName,
		InsertSQL:        insertSQL,
		SelectSQL:        selectSQL,
		SelectVersionSQL: selectVersionSQL,
		SelectAllSQL:     selectAllSQL,
		writer:           ioutil.Discard,
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}
