package client

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

func Start() {
	c := http.Client{Timeout: time.Second}
	req, err := http.NewRequest(`POST`, `http://localhost:8080`, nil)
	if err != nil {
		fmt.Printf("Error: %s\\n", err)
		return
	}
	log.Println("Request was send")
	resp, err := c.Do(req)
	if err != nil {
		fmt.Printf("Error: %s\\n", err)
		return
	}
	body, error := io.ReadAll(resp.Body)
	if error != nil {
		fmt.Println(error)
	}
	defer resp.Body.Close()
	log.Println(string(body))
}
