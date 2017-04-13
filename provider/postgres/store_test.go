package postgres_test

import (
	"context"
	"os"
	"testing"

	"github.com/savaki/eventsource/provider/postgres"
	"github.com/savaki/eventsource/provider/providertest"
	"github.com/savaki/eventsource/provider/sqlstore"
	"github.com/stretchr/testify/assert"
)

func TestStore_Save(t *testing.T) {
	ctx := context.Background()
	tableName := "entity_events"

	// Ensure table exists

	db := MustOpen()
	err := postgres.CreatePostgres(ctx, db, tableName)
	assert.Nil(t, err)
	db.Close()

	// Run provider tests

	store := sqlstore.New(tableName, Open, postgres.WithDialectPostgres(), sqlstore.WithDebug(os.Stderr))
	providertest.TestStore_Save(t, store)
}
