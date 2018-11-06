package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	mssqldb "github.com/denisenkom/go-mssqldb"
	uuid "github.com/satori/go.uuid"
)

type Config struct {
	Server   string `json:"server"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	Database string `json:"database"`
}

var (
	conf = &Config{
		Server:   "",
		Port:     1433,
		Password: "",
		Database: "",
		User:     "",
	}
)

const (
	CreateSessionIDCountTable = `If OBJECT_ID('IDCount') is null 
	CREATE TABLE IDCount(
	 ID uniqueidentifier PRIMARY KEY,
	 Count BIGINT
	);`

	CreateUpdateSessionCountStoredProc = `CREATE OR ALTER PROC dbo.GetCount(@key uniqueidentifier, @count bigint output)
		AS
		SET @count = 0;
		SELECT @count=[Count] FROM dbo.IDCount WHERE ID = @key
	`

	RPS = 2000
)

func main() {

	logger := log.New(os.Stdout, "mssql: ", 0)
	db, err := getDB(conf, logger)

	if err != nil {
		panic(err.Error())
	}

	// create table if it doesn't exist
	err = ensureTableExists(db)
	if err != nil {
		panic(err.Error())
	}

	// create the stored proc if it doesn't exist
	err = ensureStoredProcsExist(db)
	if err != nil {
		panic(err.Error())
	}

	// create a bunch of unique keys
	keys := make([]string, RPS)
	for i := 0; i < RPS; i++ {
		key := uuid.NewV4().String()
		keys[i] = key
	}

	// Send RPS queries ever second
	ticker := time.NewTicker(time.Second)
	for range ticker.C {
		for i := 0; i < RPS; i++ {
			index := i
			go func() {
				_, err := getCount(db, keys[index])
				if err != nil {
					logger.Printf("Error: %s\n", err.Error())
				}
			}()
		}
		logger.Printf("Sent %d requests", RPS)
	}
}

func ensureTableExists(db *sql.DB) error {
	_, err := db.Exec(CreateSessionIDCountTable)
	return err
}

func ensureStoredProcsExist(db *sql.DB) error {
	_, err := db.Exec(CreateUpdateSessionCountStoredProc)
	return err
}

func getCount(db *sql.DB, key string) (uint32, error) {
	tsql := `declare @Count bigint;
			exec dbo.GetCount2 @key, @Count OUTPUT;
			SELECT @COUNT;`

	var Count uint32
	err := db.QueryRow(tsql, sql.Named("key", key)).Scan(&Count)

	return Count, err
}

func getDB(c *Config, logger *log.Logger) (*sql.DB, error) {
	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s;connection timeout=30;keepalive=0;log=2", c.Server, c.User, c.Password, c.Port, c.Database)
	mssqldb.SetLogger(logger)
	connector, err := mssqldb.NewConnector(connString)
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(connector)
	db.SetMaxIdleConns(100)
	db.SetMaxOpenConns(100)
	return db, nil
}
