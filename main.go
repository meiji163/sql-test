package main

import (
	"database/sql"
	"errors"
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

	example := &Issue{
		RepoID:       "MDEwOlJlcG9zaXRvcnkyMTI2MTMwNDk",
		Number:       4754,
		count:        1,
		lastAccessed: time.Now(),
	}

	if err := createTables(db); err != nil {
		fmt.Println(err)
	}

	if err := InsertIssue(db, example); err != nil {
		fmt.Println(err)
	}
}

var ErrNotExists = errors.New("sqlite error: row doesn't exist")

type Issue struct {
	RepoID       string
	Number       int
	lastAccessed time.Time
	count        int
}

// PR functions will be essentially the same;
// should be able to unify them

//
func InsertIssue(db *sql.DB, issue *Issue) error {
	_, err := db.Exec(
		"INSERT INTO issues values(?,?,?,?)",
		issue.Number,
		issue.count,
		issue.lastAccessed,
		issue.RepoID)

	if err != nil {
		return err
	}
	return nil
}

func GetIssue(db *sql.DB, repoId string, issueNumber int) (*Issue, error) {
	row := db.QueryRow("SELECT lastAccessed,count FROM issues WHERE repoID = ? AND number = ?", repoId, issueNumber)

	var issue Issue
	if err := row.Scan(&issue.lastAccessed, &issue.count); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotExists
		}
		return nil, err
	}
	return &issue, nil
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
		lastAccessed TEXT NOT NULL,
		repoID TEXT NOT NULL UNIQUE,
		FOREIGN KEY (repoID) REFERENCES repo(ID)
	);

	CREATE TABLE IF NOT EXISTS pullrequests(
		number INTEGER PRIMARY KEY,
		count INTEGER NOT NULL,
		lastAccessed TEXT NOT NULL,
		repoID TEXT NOT NULL UNIQUE,
		FOREIGN KEY (repoID) REFERENCES repo(ID)
	);`

	_, err := db.Exec(query)
	return err
}
