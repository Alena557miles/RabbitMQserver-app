package main

import (
	database "async_serv/db"
	"log"
	"time"
)

func doEntries(i int) error {
	db, err := database.GetInstance()
	if err != nil {
		log.Println("SQL DB Connection Failed")
		return err
	}

	for i := 0; i < AmountEntries; i++ {
		_, err = db.Exec(`INSERT INTO Counts (created_at) VALUES (?)`, time.Now())
		if err != nil {
			log.Println(err)
			return err
		}
	}
	log.Printf(`created : %d million database entry `, i)
	return nil
}

func countRows() (int, error) {
	db, err := database.GetInstance()
	if err != nil {
		log.Println("SQL DB Connection Failed")
		return 0, err
	}
	var count int
	rows := db.QueryRow(`SELECT COUNT(id) FROM Counts`)
	rows.Scan(&count)
	return count, nil
}
