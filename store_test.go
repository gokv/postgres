package postgres_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/gokv/postgres"
	_ "github.com/lib/pq"
)

type String string

func (s *String) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	*s = String(str)
	return nil
}

func (s String) MarshalJSON() ([]byte, error) {
	return json.Marshal(string(s))
}

type StringCollection []*String

func (sc *StringCollection) New() json.Unmarshaler {
	s := new(String)
	*sc = append(*sc, s)
	return s
}

func newDB() *sql.DB {
	var host string
	if host = os.Getenv("POSTGRES_HOST"); host == "" {
		host = "localhost"
	}

	db, err := sql.Open("postgres", "host="+host+" user=postgres dbname=store sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	return db
}

func TestPing(t *testing.T) {
	t.Run("returns nil on a healthy connection", func(t *testing.T) {
		db := newDB()
		defer db.Close()
		s, err := postgres.New(db, "test_table")
		if err != nil {
			panic(err)
		}
		defer s.Close()

		if err := s.Ping(context.Background()); err != nil {
			t.Error(err)
		}
	})

	t.Run("returns non-nil error a failed connection", func(t *testing.T) {
		db := newDB()
		s, err := postgres.New(db, "test_table")
		if err != nil {
			panic(err)
		}
		defer s.Close()

		db.Close()

		if err := s.Ping(context.Background()); err == nil {
			t.Errorf("expected error, found <nil>")
		}
	})
}

func TestGet(t *testing.T) {
	db := newDB()
	defer db.Close()
	s, err := postgres.New(db, "test_table")
	if err != nil {
		panic(err)
	}
	defer s.Close()

	if _, err := db.Exec("DELETE FROM test_table"); err != nil {
		panic(err)
	}

	if _, err := db.Exec("INSERT INTO test_table (k, v) VALUES ($1, $2)", "key", `"the value"`); err != nil {
		panic(err)
	}

	var v String
	ok, err := s.Get(context.Background(), "key", &v)

	if !ok {
		t.Error("expected OK to be true, found false")
	}

	if err != nil {
		t.Errorf("expected err to be nil, found `%v`", err)
	}

	if have, want := string(v), "the value"; have != want {
		t.Errorf("expected value to be %q, found %q", want, have)
	}
}

func TestGetAll(t *testing.T) {
	db := newDB()
	defer db.Close()
	s, err := postgres.New(db, "test_table")
	if err != nil {
		panic(err)
	}
	defer s.Close()

	if _, err := db.Exec("DELETE FROM test_table"); err != nil {
		panic(err)
	}

	if _, err := db.Exec("INSERT INTO test_table (k, v) VALUES ($1, $2), ($3, $4)", "key0", `"value0"`, "key1", `"value1"`); err != nil {
		panic(err)
	}

	var v StringCollection
	err = s.GetAll(context.Background(), &v)

	if err != nil {
		t.Errorf("expected err to be nil, found `%v`", err)
	}

	if len(v) != 2 {
		t.Errorf("expected v to have length 2, found %d", len(v))
	}
	for i := range v {
		if want, have := fmt.Sprint("value", i), string(*v[i]); have != want {
			t.Errorf("item %d: expected %q, found %q", i, want, have)
		}
	}
}

func TestAddSetUpdate(t *testing.T) {
	db := newDB()
	defer db.Close()
	s, err := postgres.New(db, "test_table")
	if err != nil {
		panic(err)
	}
	defer s.Close()

	reset := func() {
		if _, err := db.Exec("DELETE FROM test_table"); err != nil {
			panic(err)
		}
	}

	succeeds, fails := true, false

	t.Run("when passed a new key", func(t *testing.T) {
		for _, tc := range [...]struct {
			name        string
			action      func(context.Context, string, json.Marshaler) error
			expectation bool
		}{
			{
				"Add succeeds",
				s.Add,
				succeeds,
			},
			{
				"Set succeeds",
				s.Set,
				succeeds,
			},
			{
				"Update fails",
				s.Update,
				fails,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				var (
					ctx      = context.Background()
					newValue = String("new value")
				)

				reset()

				err := tc.action(ctx, "key", newValue)

				var v string
				sqlErr := db.QueryRow("SELECT v FROM test_table WHERE k=$1", "key").Scan(&v)
				if sqlErr != nil && sqlErr != sql.ErrNoRows {
					panic(sqlErr)
				}

				if tc.expectation == succeeds {
					if err != nil {
						t.Errorf("unexpected error: `%v`", err)
					}

					if sqlErr != nil {
						t.Errorf("unexpected error querying the DB: `%v`", sqlErr)
					}

					if have, want := v, `"new value"`; have != want {
						t.Errorf("query expected to return %q, found %q", want, have)
					}
				} else {
					if err == nil {
						t.Errorf("expected error, found <nil>")
					}

					if have, want := sqlErr, sql.ErrNoRows; have != want {
						t.Errorf("expected error `%v`, found `%v`", want, have)
					}

					if have, want := v, ""; have != want {
						t.Errorf("query expected to return %q, found %q", want, have)
					}
				}
			})
		}
	})

	t.Run("when passed an existing key", func(t *testing.T) {
		for _, tc := range [...]struct {
			name        string
			action      func(context.Context, string, json.Marshaler) error
			expectation bool
		}{
			{
				"Add fails",
				s.Add,
				fails,
			},
			{
				"Set succeeds",
				s.Set,
				succeeds,
			},
			{
				"Update succeeds",
				s.Update,
				succeeds,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				var (
					ctx      = context.Background()
					newValue = String("new value")
					preset   = `"pre-existing value"`
				)

				reset()

				if _, err := db.Exec("INSERT INTO test_table (k, v) VALUES ($1, $2)", "key", preset); err != nil {
					panic(err)
				}

				err := tc.action(ctx, "key", newValue)

				var v string
				sqlErr := db.QueryRow("SELECT v FROM test_table WHERE k=$1", "key").Scan(&v)
				if sqlErr != nil {
					panic(sqlErr)
				}

				if tc.expectation == succeeds {
					if err != nil {
						t.Errorf("unexpected error: `%v`", err)
					}

					if have, want := v, `"new value"`; have != want {
						t.Errorf("query expected to return %q, found %q", want, have)
					}
				} else {
					if err == nil {
						t.Errorf("expected error, found <nil>")
					}

					if have, want := v, preset; have != want {
						t.Errorf("query expected to return %q, found %q", want, have)
					}
				}
			})
		}
	})
}

func TestDelete(t *testing.T) {
	db := newDB()
	defer db.Close()
	s, err := postgres.New(db, "test_table")
	if err != nil {
		panic(err)
	}
	defer s.Close()

	if _, err := db.Exec("DELETE FROM test_table"); err != nil {
		panic(err)
	}

	if _, err := db.Exec("INSERT INTO test_table (k, v) VALUES ($1, $2)", "key", `"the value"`); err != nil {
		panic(err)
	}

	if err := s.Delete(context.Background(), "key"); err != nil {
		t.Errorf("expected err to be nil, found `%v`", err)
	}

	var v string
	if sqlErr := db.QueryRow("SELECT v FROM test_table WHERE k=$1", "key").Scan(&v); sqlErr != sql.ErrNoRows {
		t.Errorf("expected `%v`, found `%v`", sql.ErrNoRows, sqlErr)
	}
}
