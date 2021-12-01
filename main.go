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
		OwnerName:        "cli",
		OwnerID:          "MDEyOk9yZ2FuaXphdGlvbjU5NzA0NzEx",
		Name:             "cli",
		ID:               "MDEwOlJlcG9zaXRvcnkyMTI2MTMwNDk",
		lastUpdatedIssue: time.Now(),
	}

	example := []*DBEntry{
		{
			Number: 4827,
			Title:  "Add a --no-color flag to the cli",
			ID:     "I_kwDODKw3uc4_jzMw",
			Repo:   cliRepo,
			Stats:  CountEntry{Count: 1, LastAccessed: time.Now()},
		},
		{
			Number: 4567,
			Title:  "Add a color flag to the cli",
			ID:     "I_kwDODKw3uc49blCN",
			Repo:   cliRepo,
			Stats:  CountEntry{Count: 1000000, LastAccessed: time.Now()},
			IsPR:   true,
		},
		{
			Number: 4746,
			Title:  "Don't use the --no-color flag",
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

	issues, err := GetIssues(db, cliRepo.ID)
	if err != nil {
		log.Fatal("GET error: ", err)
	}

	for _, issue := range issues {
		fmt.Printf("%+v\n", issue)
	}

	prs, err := GetPullRequests(db, cliRepo.ID)
	if err != nil {
		log.Fatal("GET error: ", err)
	}

	for _, pr := range prs {
		fmt.Printf("%+v\n", pr)
	}

	// exists, err := RepoExists(db, "asd;lkfja;lsd")
	// if err != nil {
	// 	log.Fatal("GET error: ", err)
	// }
	// fmt.Println(exists)
}

// DBEntry is a frecency entry for a issue or PR
// PRs are also issues, so we can store them in one table
// IDs are the graphQL IDs
type DBEntry struct {
	Number int
	Title  string
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
	OwnerID          string
	OwnerName        string
	ID               string
	lastUpdatedIssue time.Time
	Name             string
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
		return tx.Rollback()
	}
	tx.Commit()
	return nil
}

// Create new Issue/PR entry in database
func InsertEntry(db *sql.DB, entry *DBEntry) error {

	// insert the owner if it doesn't exist yet
	ownerExists, err := OwnerExists(db, entry.Repo.OwnerID)
	if err != nil {
		return err
	}
	if !ownerExists {
		err := InsertOwner(db, entry.Repo)
		if err != nil {
			return err
		}
	}

	// insert the repo if it doesn't exist yet
	repoExists, err := RepoExists(db, entry.Repo.ID)
	if err != nil {
		return err
	}
	if !repoExists {
		err := InsertRepo(db, &entry.Repo)
		if err != nil {
			return err
		}
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	_, err = tx.Exec("INSERT INTO issues values(?,?,?,?,?,?,?)",
		entry.ID,
		entry.Title,
		entry.Number,
		entry.Stats.Count,
		entry.Stats.LastAccessed.Unix(),
		entry.Repo.ID,
		entry.IsPR)

	if err != nil {
		return tx.Rollback()
	}
	tx.Commit()
	return nil
}

func InsertOwner(db *sql.DB, repository Repository) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	//insert the owner if it doesn't exist yet
	_, err = tx.Exec("INSERT INTO owners values(?,?)",
		repository.OwnerID,
		repository.OwnerName)

	if err != nil {
		return tx.Rollback()
	}
	tx.Commit()
	return nil
}

func InsertRepo(db *sql.DB, repo *Repository) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	_, err = tx.Exec("INSERT INTO repos values(?,?,?,?)", repo.ID, repo.Name, repo.OwnerID, repo.lastUpdatedIssue.Unix())
	if err != nil {
		return tx.Rollback()
	}
	tx.Commit()
	return nil
}

func OwnerExists(db *sql.DB, ownerID string) (bool, error) {
	row := db.QueryRow("SELECT id FROM owners WHERE id = ? LIMIT 1", ownerID)
	var id string
	err := row.Scan(&id)
	if err == nil {
		return true, nil
	}

	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return false, err
}

func RepoExists(db *sql.DB, repoID string) (bool, error) {
	row := db.QueryRow("SELECT id FROM repos WHERE id = ?", repoID)
	var id string
	err := row.Scan(&id)
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

func getEntries(db *sql.DB, repoID string, IsPR int) ([]*DBEntry, error) {
	query := `
	SELECT number,lastAccessed,count,isPR,title FROM issues 
		WHERE repoID = ? 
		AND isPR = ?
		ORDER BY lastAccessed DESC`
	rows, err := db.Query(query, repoID, IsPR)
	if err != nil {
		return nil, err
	}

	var entries []*DBEntry
	for rows.Next() {
		entry := DBEntry{}
		var unixTime int64
		if err := rows.Scan(&entry.Number, &unixTime, &entry.Stats.Count, &entry.IsPR, &entry.Title); err != nil {
			return nil, err
		}
		entry.Stats.LastAccessed = time.Unix(unixTime, 0)
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
		lastUpdatedIssue INTEGER NOT NULL,
 		FOREIGN KEY (ownerID) REFERENCES owner(id)
	);
	
	CREATE TABLE IF NOT EXISTS issues(
		id TEXT NOT NULL,
		title TEXT NOT NULL,
		number INTEGER PRIMARY KEY,
		count INTEGER NOT NULL,
		lastAccessed INTEGER NOT NULL,
		repoID TEXT NOT NULL,
		isPR BOOLEAN NOT NULL DEFAULT 0,
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
