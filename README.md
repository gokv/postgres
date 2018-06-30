# gokv/postgres
[![GoDoc](https://godoc.org/github.com/gokv/postgres?status.svg)](https://godoc.org/github.com/gokv/postgres)
[![Build Status](https://travis-ci.org/gokv/postgres.svg?branch=master)](https://travis-ci.org/gokv/postgres)


A reflectionless [gokv/store](https://github.com/gokv/store) PostgreSQL client.

This package is not ready for production use.

---

The aim of this package is to provide an idiomatic Go API to the SQL persistence. The data is accepted in a key/value format, where the value implements JSON marshaler/unmarshaler, and is persisted as [`JSONb`](https://www.postgresql.org/docs/10/static/datatype-json.html).

A typical approach to database abstraction is the ORM (Object-Relational Mapper): a piece of software that maps your domain models to some SQL code. In this case, the actual mapping functions are defined by the consumer: the persisted types need to provide an explicit implementation of `MarhalJSON` and `UnmarshalJSON`.

Here is how the usage experience was designed:

```Go
// Instantiate a new PostgreSQL store
s, _ = postgres.New(db, "table_name")

// store defines the methods required in this function, or in this package.
type store interface {
  Add(ctx context.Context, id interface{}, v json.Marshaler) error
}

// SaveNewBook accepts the postgres.Store as a locally-defined interface.
// Later on, another Store implementation could be passed instead, for example
// using Redis.
//
// The `book` type must explicitly implement `json.Marshaler`
func SaveNewBook(s store, newBook book) error {
  return s.Add(req.Context(), uuid.New(), newBook)
}
```

## Requisites

- Go v1.8 (for context.Context integration in the stdlib)
- Postgres v9.5 (first version to provide upsert via the [`ON CONFLICT`](https://www.postgresql.org/docs/9.5/static/sql-insert.html#SQL-ON-CONFLICT) clause)

## Use

The `New` initialisator prepares SQL statements against the given `*sql.DB`, and exposes methods that act upon those prepared statements.

Here's how to use it:

```Go
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"time"

	"github.com/gokv/postgres"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

// User is the type to persist
type User struct {
	Firstname string
	Lastname  string
	CreatedAt time.Time
}

// The type has to explicitly implement json.Marshaler
func (u User) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Firstname string    `json:"firstname"`
		Lastname  string    `json:"lastname"`
		CreatedAt time.Time `json:"created_at"`
	}{
		Firstname: u.Firstname,
		Lastname:  u.Lastname,
		CreatedAt: u.CreatedAt,
	})
}

func main() {
	// Create the connection abstraction
	db, err := sql.Open("postgres", "host=postgres user=username password=secret dbname=store")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Pass the *sql.DB and the name of the table the Store will act on
	s, err := postgres.New(db, "users")
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()

	// Add a new entry
	err = s.Add(context.Context(), uuid.New(), User{
		Firstname: "Giacomo",
		Lastname:  "Leopardi",
		CreatedAt: time.Now(),
	})
	if err != nil {
		log.Fatal(err)
	}
}
```

## Roadmap

[x] Basic CRUD
[ ] Search
