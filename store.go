package postgres

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/gokv/store"
	"github.com/google/uuid"
)

// Store holds the SQL statements prepared against a Postgresql table.
// Initialise with New.
type Store struct {
	getStmt    *sql.Stmt
	getAllStmt *sql.Stmt
	addStmt    *sql.Stmt
	setStmt    *sql.Stmt
	updateStmt *sql.Stmt
	deleteStmt *sql.Stmt

	ping func(context.Context) error
}

// New creates a table of name tablename if it does not exist, and prepares
// statements against it.
// The table has two columms: "k" is the TEXT primary key and "v" is a JSONb column holding the values.
func New(db *sql.DB, tablename string) (s Store, err error) {
	if _, err = db.Exec(`CREATE TABLE IF NOT EXISTS "` + tablename + `" (k TEXT NOT NULL PRIMARY KEY, v jsonb NOT NULL)`); err != nil {
		return s, err
	}

	if s.getStmt, err = db.Prepare(`SELECT v FROM "` + tablename + `" WHERE k=$1`); err != nil {
		_ = s.Close()
		return s, err
	}

	if s.getAllStmt, err = db.Prepare(`SELECT v FROM "` + tablename + `"`); err != nil {
		_ = s.Close()
		return s, err
	}

	if s.addStmt, err = db.Prepare(`INSERT INTO "` + tablename + `" (k, v) VALUES ($1, $2)`); err != nil {
		_ = s.Close()
		return s, err
	}

	if s.setStmt, err = db.Prepare(`INSERT INTO "` + tablename + `" (k, v) VALUES ($1, $2) ON CONFLICT (k) DO UPDATE SET v=$2`); err != nil {
		_ = s.Close()
		return s, err
	}

	if s.updateStmt, err = db.Prepare(`UPDATE "` + tablename + `" SET v=$2 WHERE k=$1`); err != nil {
		_ = s.Close()
		return s, err
	}

	if s.deleteStmt, err = db.Prepare(`DELETE FROM "` + tablename + `" WHERE k=$1`); err != nil {
		_ = s.Close()
		return s, err
	}

	s.ping = db.PingContext

	return s, err
}

// Close releases the resources associated with the Store.
// Returns the first error encountered while closing the prepared statements.
func (s Store) Close() (err error) {
	for _, stmt := range []*sql.Stmt{
		s.getStmt,
		s.getAllStmt,
		s.addStmt,
		s.setStmt,
		s.updateStmt,
		s.deleteStmt,
	} {
		if stmt != nil {
			if e := stmt.Close(); err == nil {
				err = e
			}
		}
	}
	return err
}

// Get retrieves a new item by key and unmarshals it into v, or returns false if
// not found.
// Err is non-nil in case of failure.
func (s Store) Get(ctx context.Context, k string, v json.Unmarshaler) (bool, error) {
	var b []byte
	if err := s.getStmt.QueryRowContext(ctx, k).Scan(&b); err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	return true, v.UnmarshalJSON(b)
}

// GetAll appends to c every item in the store.
// Err is non-nil in case of failure.
func (s Store) GetAll(ctx context.Context, c store.Collection) error {
	rows, err := s.getAllStmt.QueryContext(ctx)
	if err != nil {
		return err
	}
	defer rows.Close()

	var b []byte
	for rows.Next() {
		if err = rows.Scan(&b); err != nil {
			return err
		}
		if err = c.New().UnmarshalJSON(b); err != nil {
			return err
		}
	}
	return rows.Err()
}

// Add persists a new object and returns its unique UUIDv4 key.
// Err is non-nil in case of failure.
func (s Store) Add(ctx context.Context, v json.Marshaler) (string, error) {
	b, err := v.MarshalJSON()
	if err != nil {
		return "", err
	}

	k := uuid.New().String()

	_, err = s.addStmt.ExecContext(ctx, k, b)
	return k, err
}

func (s Store) Set(ctx context.Context, k string, v json.Marshaler) error {
	b, err := v.MarshalJSON()
	if err != nil {
		return err
	}

	_, err = s.setStmt.ExecContext(ctx, k, b)
	return err
}

// Update assigns the given value to the given key, if it exists.
// Err is non-nil if the key was not already present, or in case of failure.
func (s Store) Update(ctx context.Context, k string, v json.Marshaler) error {
	b, err := v.MarshalJSON()
	if err != nil {
		return err
	}

	res, err := s.updateStmt.ExecContext(ctx, k, b)
	if err != nil {
		return err
	}

	n, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if n < 1 {
		return store.ErrNoRows
	}

	return nil
}

func (s Store) Delete(ctx context.Context, k string) error {
	res, err := s.deleteStmt.ExecContext(ctx, k)
	if err != nil {
		return err
	}

	n, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if n < 1 {
		return store.ErrNoRows
	}

	return nil
}

func (s Store) Ping(ctx context.Context) error {
	return s.ping(ctx)
}
