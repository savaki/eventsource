package mysql_test

import (
	"context"
	"os"
	"testing"

	"github.com/savaki/eventsource"
	"github.com/savaki/eventsource/provider/mysql"
	"github.com/savaki/eventsource/provider/providertest"
	"github.com/savaki/eventsource/provider/sqlstore"
	"github.com/stretchr/testify/assert"
)

type EntitySetFirst struct {
	eventsource.Model
	First string
}

type EntitySetLast struct {
	eventsource.Model
	Last string
}

func TestStore_Save(t *testing.T) {
	ctx := context.Background()
	tableName := "entity_events"

	// Ensure table exists

	db := MustOpen()
	err := mysql.CreateMySQL(ctx, db, tableName)
	assert.Nil(t, err)
	db.Close()

	// Run provider tests

	store := sqlstore.New(tableName, Open, sqlstore.WithDebug(os.Stderr))
	providertest.TestStore_Save(t, store)
}
