package main

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func main() {
	db, err := sql.Open("sqlite3", "test.db")
	if err != nil {
		log.Fatal(err)
	}

	cliRepo := Repository{
		OwnerName: "cli",
		OwnerID:   "MDEyOk9yZ2FuaXphdGlvbjU5NzA0NzEx",
		Name:      "cli",
		ID:        "MDEwOlJlcG9zaXRvcnkyMTI2MTMwNDk",
	}

	example := []*DBEntry{
		{
			Number: 4827,
			ID:     "I_kwDODKw3uc4_jzMw",
			Repo:   cliRepo,
			Stats:  CountEntry{Count: 1, LastAccessed: time.Now()},
		},
		{
			Number: 4567,
			ID:     "I_kwDODKw3uc49blCN",
			Repo:   cliRepo,
			Stats:  CountEntry{Count: 1000000, LastAccessed: time.Now()},
		},
		{
			Number: 4746,
			ID:     "I_kwDODKw3uc4-8U58",
			Repo:   cliRepo,
			Stats:  CountEntry{Count: 1, LastAccessed: time.Now()},
		},
	}

	if err := createTables(db); err != nil {
		log.Fatal(err)
	}

	for _, issue := range example {
		if err := InsertEntry(db, issue); err != nil {
			log.Fatal(err)
		}
	}

	// update an entry
	updated := example[0]
	updated.Stats.LastAccessed = time.Now().AddDate(0, -1, 2)
	updated.Stats.Count++
	if err := UpdateEntry(db, updated); err != nil {
		log.Fatal(err)
	}

	_, err = GetIssues(db, cliRepo.ID)
	if err != nil {
		log.Fatal("GET error: ", err)
	}

	exists, err := RepoExists(db, "asd;lkfja;lsd")
	fmt.Println(exists)
}

// DBEntry is a frecency entry for a issue or PR
// PRs are also issues, so we can store them in one table
// IDs are the graphQL IDs
type DBEntry struct {
	Number int
	ID     string
	Repo   Repository
	Stats  CountEntry
	IsPR   bool
}

type CountEntry struct {
	LastAccessed time.Time
	Count        int
}

type Repository struct {
	OwnerID   string
	OwnerName string
	ID        string
	Name      string
}

// Update the entry's timestamp and frequency count
func UpdateEntry(db *sql.DB, updated *DBEntry) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	query := "UPDATE issues SET lastAccessed = ?, count = ? WHERE repoId = ? AND number = ?"
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

// Create new Issue/PR entry in database
func InsertEntry(db *sql.DB, entry *DBEntry) error {
	repoExists, err := RepoExists(db, entry.Repo.ID)
	if !repoExists {
		InsertRepo(db, &entry.Repo)
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	_, err = tx.Exec("INSERT INTO issues values(?,?,?,?,?,?)",
		entry.ID,
		entry.Number,
		entry.Stats.Count,
		entry.Stats.LastAccessed.Unix(),
		entry.Repo.ID,
		entry.IsPR)

	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()
	return nil
}

func InsertRepo(db *sql.DB, repo *Repository) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	// insert the owner entry if it doesn't exist yet
	ownerExists, err := OwnerExists(db, repo.OwnerID)
	if err != nil {
		tx.Rollback()
		return err
	}
	if !ownerExists {
		_, err = tx.Exec("INSERT INTO owners values(?,?)", repo.OwnerName, repo.ID)
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	_, err = tx.Exec("INSERT INTO repos values(?,?,?)", repo.ID, repo.Name, repo.OwnerID)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()
	return nil
}

func OwnerExists(db *sql.DB, ownerID string) (bool, error) {
	row := db.QueryRow("SELECT 1 FROM owners WHERE id = ? LIMIT 1", ownerID)
	err := row.Scan()
	if err == nil {
		return true, nil
	}

	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return false, err
}

func RepoExists(db *sql.DB, repoID string) (bool, error) {
	row := db.QueryRow("SELECT 1 FROM repos WHERE id = ? LIMIT 1", repoID)
	err := row.Scan()
	if err == nil {
		return true, nil
	}

	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return false, err
}

// Retrieve all issues under the repo with given ID
func GetIssues(db *sql.DB, repoID string) ([]*DBEntry, error) {
	return getEntries(db, repoID, 0)
}

// Retrieve all PRs under the repo with given ID
func GetPullRequests(db *sql.DB, repoID string) ([]*DBEntry, error) {
	return getEntries(db, repoID, 1)
}

func getEntries(db *sql.DB, repoID string, getPRs int) ([]*DBEntry, error) {
	query := `
	SELECT number,lastAccessed,count,isPR FROM issues 
		WHERE repoID = ? 
		AND isPR = ?
		ORDER BY lastAccessed DESC`
	rows, err := db.Query(query, repoID, getPRs)
	if err != nil {
		return nil, err
	}

	var entries []*DBEntry
	for rows.Next() {
		entry := DBEntry{}
		var unixTime int64
		if err := rows.Scan(&entry.Number, &unixTime, &entry.Stats.Count, &entry.IsPR); err != nil {
			return nil, err
		}
		entry.Stats.LastAccessed = time.Unix(unixTime, 0)
		fmt.Printf("%+v\n", entry)
		entries = append(entries, &entry)
	}

	return entries, nil
}

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
	
	CREATE TABLE IF NOT EXISTS issues(
		id TEXT NOT NULL,
		number INTEGER PRIMARY KEY,
		count INTEGER NOT NULL,
		lastAccessed INTEGER NOT NULL,
		repoID TEXT NOT NULL,
		isPR BOOLEAN NOT NULL 
			CHECK (isPR IN (0, 1)) 
			DEFAULT 0,
		FOREIGN KEY (repoID) REFERENCES repo(ID)
	);

	CREATE INDEX IF NOT EXISTS 
	frecent ON issues(lastAccessed, count);
	`
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
