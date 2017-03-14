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
	sqlInsert        = `INSERT INTO {{ .TableName }} (event_key, event_type, data, version, at) VALUES (?, ?, ?, ?, ?)`
	sqlSelectVersion = `SELECT event_type, data, version, at FROM {{ .TableName }} WHERE event_key = ? and version <= ?`
	sqlSelect        = `SELECT event_type, data, version, at FROM {{ .TableName }} WHERE event_key = ?`
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

func (s *Store) Save(ctx context.Context, serializer eventsource.Serializer, events ...interface{}) error {
	db, err := s.openFunc()
	if err != nil {
		return err
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	s.log("Saving", len(events), "events.")

	err = func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(s.insertSQL)
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, event := range events {
			meta, err := eventsource.Inspect(event)
			if err != nil {
				return err
			}

			data, err := serializer.Serialize(event)
			if err != nil {
				return err
			}

			s.log("Saving version,", meta.Version)
			_, err = stmt.Exec(meta.ID, meta.EventType, data, meta.Version, meta.At)
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

func (s *Store) Fetch(ctx context.Context, serializer eventsource.Serializer, aggregateID string, version int) ([]interface{}, int, error) {
	if version == 0 {
		version = math.MaxInt32
	}

	db, err := s.openFunc()
	if err != nil {
		return nil, 0, err
	}
	defer db.Close()

	s.log("Reading events with aggregrateID,", aggregateID)
	query := s.selectSQL
	if version > 0 {
		query = s.selectVersionSQL
	}
	rows, err := db.QueryContext(ctx, query, aggregateID, version)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	metas := make([]eventsource.EventMeta, 0, version+1)
	for rows.Next() {
		s.log("Scanning row")
		meta := eventsource.EventMeta{}
		data := []byte{}
		err := rows.Scan(&meta.EventType, &data, &meta.Version, &meta.At)
		if err != nil {
			return nil, 0, err
		}

		s.log("Reading version,", meta.Version)
		event, err := serializer.Deserialize(meta.EventType, data)
		if err != nil {
			return nil, 0, err
		}
		meta.Event = event

		metas = append(metas, meta)
	}

	sort.Slice(metas, func(i, j int) bool {
		return metas[i].Version < metas[j].Version
	})

	foundVersion := 0
	events := make([]interface{}, 0, len(metas))
	for _, meta := range metas {
		events = append(events, meta.Event)
		foundVersion = meta.Version
	}

	s.log("Successfully read", len(events), "events")
	return events, foundVersion, nil
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
