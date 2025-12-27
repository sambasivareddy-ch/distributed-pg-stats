package helpers

import (
	"database/sql"
	"errors"
	"fmt"

	_ "github.com/lib/pq"
)

// Connects to main postgres node
func ConnectToPostgres(host, user, database string, port int16) (*sql.DB, error) {
	connectionStr := fmt.Sprintf("dbname=%s user=%s host=%s port=%d sslmode=disable", database, user, host, port)

	connection, err := sql.Open("postgres", connectionStr)
	if err != nil {
		return nil, errors.New(err.Error())
	}

	fmt.Println("Successfully Connected to the Postgres")
	return connection, nil
}
