package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

//TODO: use transactions

func main() {
	db, err := sql.Open("sqlite3", "test.db")
	if err != nil {
		log.Fatal(err)
	}

	example := []*DBEntry{
		{
			RepoID:       "MDEwOlJlcG9zaXRvcnkyMTI2MTMwNDk",
			Number:       4754,
			count:        1,
			lastAccessed: time.Now(),
		},
		{
			RepoID:       "MDEwOlJlcG9zaXRvcnkyMTI2MTMwNDk",
			Number:       4567,
			count:        1000000,
			lastAccessed: time.Now(),
		},
		{
			RepoID:       "MDEwOlJlcG9zaXRvcnkyMTI2MTMwNDk",
			Number:       4758,
			count:        1,
			lastAccessed: time.Now(),
		},
	}

	if err := createTables(db); err != nil {
		fmt.Println(err)
	}

	for _, issue := range example {
		if err := InsertEntry(db, issue, "issues"); err != nil {
			fmt.Println(err)
		}
	}

	_, err = GetEntries(db, example[0].RepoID, "issues")
	if err != nil {
		fmt.Println("GET error: ", err)
	}
}

type DBEntry struct {
	OwnerID      string
	OwnerName    string
	RepoID       string
	RepoName     string
	Number       int
	lastAccessed time.Time
	count        int
}

// PR functions will be essentially the same;
// should be able to unify them

//
func InsertEntry(db *sql.DB, entry *DBEntry, dataType string) error {
	/**
	* Check if the PR exist, if not insert it and proceed
	*add the issue using the repoId
	 */

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	query := fmt.Sprintf("INSERT INTO %s values(?,?,?,?)", dataType)
	_, err = tx.Exec(
		query,
		entry.Number,
		entry.count,
		entry.lastAccessed.Unix(),
		entry.RepoID)

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
	if err == sql.ErrNoRows {
		return false, nil
	} else if err != nil {
		return false, err
	}
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
	if err == sql.ErrNoRows {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}

// Get all issues or PRs under a repo
func GetEntries(db *sql.DB, repoId string, dataType string) ([]*DBEntry, error) {
	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}

	query := fmt.Sprintf("SELECT number,lastAccessed,count FROM %s WHERE repoID = ?", dataType)
	rows, err := tx.Query(query, repoId)
	if err != nil {
		return nil, err
	}

	var entries []*DBEntry
	for rows.Next() {
		var entry DBEntry
		var unixTime int64
		if err := rows.Scan(&entry.Number, &unixTime, &entry.count); err != nil {
			return nil, err
		}
		entry.lastAccessed = time.Unix(unixTime, 0)
		fmt.Printf("%+v\n", entry)
		entries = append(entries, &entry)
	}

	tx.Commit()
	return entries, nil
}

func createTables(db *sql.DB) error {
	// IDs are the graphQL IDs
	// REST IDs aren't available in `gh issue view` or `gh pr view`
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
	
	CREATE TABLE IF NOT EXISTS issues(
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

	_, err := db.Exec(query)
	return err
}
