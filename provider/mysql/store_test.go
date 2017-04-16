package mysql_test

import (
	"context"
	"os"
	"testing"

	"github.com/savaki/eventsource/provider/mysql"
	"github.com/savaki/eventsource/provider/providertest"
	"github.com/savaki/eventsource/provider/sqlstore"
	"github.com/stretchr/testify/assert"
)

func TestStore_Save(t *testing.T) {
	ctx := context.Background()
	tableName := "entity_events"

	// Ensure table exists

	db := MustOpen()
	err := mysql.CreateMySQL(ctx, db, tableName)
	assert.Nil(t, err)
	defer db.Close()

	// Run provider tests

	store := sqlstore.New(tableName, db, sqlstore.WithDebug(os.Stderr))
	providertest.TestStore_Save(t, store)
}
