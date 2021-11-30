package main

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

//TODO: sanitize inputs(?)

func main() {
	db, err := sql.Open("sqlite3", "test.db")
	if err != nil {
		log.Fatal(err)
	}

	cliRepo := &Repository{
		OwnerName: "cli",
		RepoName:  "cli",
		ID:        "MDEwOlJlcG9zaXRvcnkyMTI2MTMwNDk",
		OwnerID:   "MDEyOk9yZ2FuaXphdGlvbjU5NzA0NzEx",
	}

	example := []*DBEntry{
		{
			Number: 4754,
			Repo:   cliRepo,
			Stats:  &CountEntry{Count: 1, LastAccessed: time.Now()},
		},
		{
			Number: 4567,
			Repo:   cliRepo,
			Stats:  &CountEntry{Count: 1000000, LastAccessed: time.Now()},
		},
		{
			Number: 4758,
			Repo:   cliRepo,
			Stats:  &CountEntry{Count: 1, LastAccessed: time.Now()},
		},
	}

	if err := createTables(db); err != nil {
		log.Fatal(err)
	}

	for _, issue := range example {
		if err := InsertEntry(db, issue, "issue"); err != nil {
			log.Fatal(err)
		}
	}

	// update an entry
	updated := example[0]
	updated.Stats.LastAccessed = time.Now().AddDate(0, -1, 2)
	updated.Stats.Count++
	if err := UpdateEntry(db, updated, "issue"); err != nil {
		log.Fatal(err)
	}

	_, err = GetEntries(db, cliRepo.ID, "issue")
	if err != nil {
		log.Fatal("GET error: ", err)
	}
}

// DBEntry is a frecency entry for a issue or PR
type DBEntry struct {
	Number int // the issue or PR ID
	Repo   *Repository
	Stats  *CountEntry
}

type CountEntry struct {
	LastAccessed time.Time
	Count        int
}

type Repository struct {
	OwnerID   string
	OwnerName string
	ID        string
	RepoName  string
}

// Update the entry's timestamp and frequency count
func UpdateEntry(db *sql.DB, updated *DBEntry, dataType string) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	query := fmt.Sprintf("UPDATE %s SET lastAccessed = ?, count = ? WHERE repoId = ? AND number = ?", dataType)
	stats := updated.Stats
	_, err = tx.Exec(
		query,
		stats.LastAccessed.Unix(),
		stats.Count,
		updated.Repo.ID,
		updated.Number)

	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()
	return nil
}

func InsertEntry(db *sql.DB, entry *DBEntry, dataType string) error {
	/*
	 * Check if the PR exist, if not insert it and proceed
	 * add the issue using the repoId
	 */

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	query := fmt.Sprintf("INSERT INTO %s values(?,?,?,?)", dataType)
	_, err = tx.Exec(
		query,
		entry.Number,
		entry.Stats.Count,
		entry.Stats.LastAccessed.Unix(),
		entry.Repo.ID)

	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()
	return nil
}

func OwnerExists(db *sql.DB, ownerID string) (bool, error) {
	tx, err := db.Begin()
	if err != nil {
		return false, err
	}
	query := fmt.Sprintf("SELECT id FROM owners WHERE id = '%s'", ownerID)
	row := tx.QueryRow(query)
	var id string
	err = row.Scan(&id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			tx.Commit()
			return false, nil
		}
		tx.Rollback()
		return false, err
	}
	tx.Commit()
	return true, nil
}

func RepoExists(db *sql.DB, repoID string) (bool, error) {
	tx, err := db.Begin()
	if err != nil {
		return false, err
	}
	query := fmt.Sprintf("SELECT id FROM repos WHERE id = '%s'", repoID)
	row := tx.QueryRow(query)
	var id string
	err = row.Scan(&id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			tx.Commit()
			return false, nil
		}
		tx.Rollback()
		return false, err
	}
	tx.Commit()
	return true, nil
}

// Get all issues or PRs under a repo
func GetEntries(db *sql.DB, repoId string, dataType string) ([]*DBEntry, error) {
	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}

	query := fmt.Sprintf("SELECT number,lastAccessed,count FROM %s WHERE repoID = ? ORDER BY lastAccessed DESC", dataType)
	rows, err := tx.Query(query, repoId)
	if err != nil {
		return nil, err
	}

	var entries []*DBEntry
	for rows.Next() {
		var entry DBEntry
		var unixTime int64
		if err := rows.Scan(&entry.Number, &unixTime, &entry.Stats.Count); err != nil {
			return nil, err
		}
		entry.Stats.LastAccessed = time.Unix(unixTime, 0)
		fmt.Printf("%+v\n", entry)
		entries = append(entries, &entry)
	}

	tx.Commit()
	return entries, nil
}

// IDs are the graphQL IDs, since REST IDs aren't available in `gh issue view` or `gh pr view`
// timestamps are unix (s)
func createTables(db *sql.DB) error {
	query := `
	CREATE TABLE IF NOT EXISTS owners( 
		id TEXT PRIMARY KEY,
		name TEXT NOT NULL UNIQUE 	 
	);

	CREATE TABLE IF NOT EXISTS repos(
 		id TEXT PRIMARY KEY,
 		name TEXT NOT NULL,
		ownerID TEXT NOT NULL UNIQUE, 
 		FOREIGN KEY (ownerID) REFERENCES owner(id)
	);
	
	CREATE TABLE IF NOT EXISTS issue(
		number INTEGER PRIMARY KEY,
		count INTEGER NOT NULL,
		lastAccessed INTEGER NOT NULL,
		repoID TEXT NOT NULL,
		FOREIGN KEY (repoID) REFERENCES repo(ID)
	);

	CREATE TABLE IF NOT EXISTS pullrequests(
		number INTEGER PRIMARY KEY,
		count INTEGER NOT NULL,
		lastAccessed INTEGER NOT NULL,
		repoID TEXT NOT NULL,
		FOREIGN KEY (repoID) REFERENCES repo(ID)
	);`

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	if _, err = tx.Exec(query); err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()
	return err
}
