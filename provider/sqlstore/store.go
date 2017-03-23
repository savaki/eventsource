package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"sort"
	"time"

	"github.com/savaki/eventsource"
)

const (
	sqlInsert        = `INSERT INTO {{ .TableName }} (id, version, data, at) VALUES (?, ?, ?, ?)`
	sqlSelectVersion = `SELECT version, data, at FROM {{ .TableName }} WHERE id = ? and version <= ?`
	sqlSelect        = `SELECT version, data, at FROM {{ .TableName }} WHERE id = ?`
)

type OpenFunc func() (*sql.DB, error)

type Store struct {
	openFunc         OpenFunc
	tableName        string
	insertSQL        string
	selectSQL        string
	selectVersionSQL string
	debug            bool
	writer           io.Writer
}

func (s *Store) Save(ctx context.Context, aggregateID string, records ...eventsource.Record) error {
	db, err := s.openFunc()
	if err != nil {
		return err
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	s.log("Saving", len(records), "events.")

	err = func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(s.insertSQL)
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
		return tx.Rollback()
	}
}

func (s *Store) Fetch(ctx context.Context, aggregateID string, version int) (eventsource.History, error) {
	if version == 0 {
		version = math.MaxInt32
	}

	db, err := s.openFunc()
	if err != nil {
		return eventsource.History{}, err
	}
	defer db.Close()

	s.log("Reading events with aggregrateID,", aggregateID)
	query := s.selectSQL
	if version > 0 {
		query = s.selectVersionSQL
	}
	rows, err := db.QueryContext(ctx, query, aggregateID, version)
	if err != nil {
		return eventsource.History{}, err
	}
	defer rows.Close()

	s.log("Scanning rows")
	history := make(eventsource.History, 0, version+1)
	for rows.Next() {
		s.log("Scanning row")
		meta := eventsource.EventMeta{}
		data := []byte{}
		err := rows.Scan(&meta.Version, &data, &meta.At)
		if err != nil {
			return eventsource.History{}, err
		}

		s.log("Reading version,", meta.Version)
		history = append(history, eventsource.Record{
			Version: meta.Version,
			At:      meta.At,
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

func New(tableName string, openFunc OpenFunc, opts ...Option) *Store {
	insertSQL := reTableName.ReplaceAllString(sqlInsert, tableName)
	selectSQL := reTableName.ReplaceAllString(sqlSelect, tableName)
	selectVersionSQL := reTableName.ReplaceAllString(sqlSelectVersion, tableName)

	s := &Store{
		openFunc:         openFunc,
		tableName:        tableName,
		insertSQL:        insertSQL,
		selectSQL:        selectSQL,
		selectVersionSQL: selectVersionSQL,
		writer:           ioutil.Discard,
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}
