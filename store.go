package postgres

import (
	"context"
	"database/sql"
	"encoding"
)

type Store struct {
	ping       func() error
	stmtSelect *sql.Stmt
	stmtInsert *sql.Stmt
	stmtKeys   *sql.Stmt
}

// New creates a table of name tablename if it does not exist, and prepares
// statements against it.
func New(db *sql.DB, tablename string) (s Store, err error) {
	if _, err = db.Exec(`CREATE TABLE IF NOT EXISTS "` + tablename + `" (key text PRIMARY KEY, value bytea)`); err != nil {
		return
	}

	if s.stmtSelect, err = db.Prepare(`SELECT value FROM "` + tablename + `" WHERE key=$1`); err != nil {
		_ = s.Close()
		return
	}

	if s.stmtInsert, err = db.Prepare(`INSERT INTO "` + tablename + `" (key, value) VALUES ($1, $2)`); err != nil {
		_ = s.Close()
		return
	}

	if s.stmtKeys, err = db.Prepare(`SELECT key FROM "` + tablename + `"`); err != nil {
		_ = s.Close()
		return
	}

	s.ping = db.Ping

	return
}

func (s Store) Ping() (err error) {
	return s.ping()
}

// Close releases the resources associated with the Client.
// Returns the first error encountered while closing the prepared statements.
func (s Store) Close() (err error) {
	for _, stmt := range []*sql.Stmt{
		s.stmtSelect,
		s.stmtInsert,
		s.stmtKeys,
	} {
		if stmt != nil {
			if e := stmt.Close(); err == nil {
				err = e
			}
		}
	}
	return
}

// Get returns the value corresponding the key, and a nil error.
// If no match is found, returns (false, nil).
func (s Store) Get(key string, v encoding.BinaryUnmarshaler) (ok bool, err error) {
	var b []byte
	err = s.stmtSelect.QueryRow(key).Scan(&b)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return
	}

	return true, v.UnmarshalBinary(b)
}

// Add assigns the given value to the given key.
// Returns error if key is already assigned.
func (s Store) Add(key string, v encoding.BinaryMarshaler) error {
	b, err := v.MarshalBinary()
	if err != nil {
		return err
	}

	_, err = s.stmtInsert.Exec(key, b)

	return err
}

// Keys lists all the stored keys.
func (s Store) Keys(ctx context.Context) (<-chan string, <-chan error) {
	var (
		keys = make(chan string)
		errs = make(chan error, 2)
	)

	rows, err := s.stmtKeys.QueryContext(ctx)
	if err != nil {
		errs <- err
		close(errs)
		close(keys)
		return keys, errs
	}

	go func() {
		defer close(errs)
		defer close(keys)
		defer func() {
			if err := rows.Close(); err != nil {
				errs <- err
			}
		}()

		for rows.Next() {
			var key string
			if err := rows.Scan(&key); err != nil {
				errs <- err
			}
			keys <- key
		}

		if err := rows.Err(); err != nil {
			errs <- err
		}
	}()

	return keys, errs
}
