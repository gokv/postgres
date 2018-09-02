package postgres

import "database/sql"

type Option func(db *sql.DB, tablename string) error

func WithCreateTable(db *sql.DB, tablename string) error {
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS "` + tablename + `" (k TEXT NOT NULL PRIMARY KEY, v jsonb NOT NULL)`)
	return err
}
