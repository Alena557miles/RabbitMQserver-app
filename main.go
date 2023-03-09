package main

import (
	"async_serv/client"
	"time"
)

func main() {
	client.Start()
	//time.Sleep(2 * time.Second)
	client.Start()
	time.Sleep(2 * time.Second)
	client.Start()
	time.Sleep(2000 * time.Second)
	client.Start()
}
