package databaseSQL

import (
	"database/sql"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
	"log"
	"os"
)

type database struct {
	db *sql.DB
}

var instance *database

func GetInstance() (*sql.DB, error) {
	err := godotenv.Load()
	if err != nil {
		log.Println("Error loading .env file")
	}
	if instance == nil {
		cfg := mysql.Config{
			User:   os.Getenv("DB_USER"),
			Passwd: os.Getenv("DB_PASSWORD"),
			Net:    "tcp",
			Addr:   os.Getenv("DB_ADDR"),
			DBName: os.Getenv("DB_NAME"),
		}
		connString := cfg.FormatDSN()
		db, err := sql.Open("mysql", connString)
		if err != nil {
			return nil, fmt.Errorf("error opening database connection: %s", err)
		}
		fmt.Println("Creating single DB instance now.")
		err = db.Ping()
		if err != nil {
			log.Println("error pinging database:")
			return nil, err
		}
		instance = &database{db: db}
		return instance.db, nil
	} else {
		return instance.db, nil
	}
}
func Close() error {
	if instance == nil {
		return nil
	}
	err := instance.db.Close()
	if err != nil {
		log.Println("error closing database connection")
		return err
	}
	instance = nil
	return nil
}
