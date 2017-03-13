package sqlstore_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/savaki/eventsource/provider/sqlstore"
	"github.com/stretchr/testify/assert"
)

type Config struct {
	Username string
	Password string
	Hostname string
	Port     string
	Database string
}

func ConnectString(cfg Config) string {
	return fmt.Sprintf("%v:%v@tcp(%v:%v)/%v?charset=utf8&parseTime=True&loc=Local",
		cfg.Username,
		cfg.Password,
		cfg.Hostname,
		cfg.Port,
		cfg.Database,
	)
}

var connectString = ConnectString(Config{
	Username: getOrElse("DB_USERNAME", "eventsource"),
	Password: getOrElse("DB_PASSWORD", "password"),
	Hostname: getOrElse("DB_HOSTNAME", "127.0.0.1"),
	Port:     getOrElse("DB_PORT", "3306"),
	Database: getOrElse("DB_DATABASE", "eventsource"),
})

func getOrElse(key, defaultValue string) string {
	v := os.Getenv(key)
	if v == "" {
		return defaultValue
	}

	return v
}

func Open() (*sql.DB, error) {
	return sql.Open("mysql", connectString)
}

func MustOpen() *sql.DB {
	db, err := Open()
	if err != nil {
		log.Fatalln(err)
	}
	return db
}

func TestCreateTable(t *testing.T) {
	db := MustOpen()
	defer db.Close()

	ctx := context.Background()
	err := sqlstore.CreateMySQL(ctx, db, "thing_events")
	assert.Nil(t, err)
}
