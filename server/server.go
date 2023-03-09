package main

import (
	"async_serv/consume"
	"async_serv/produce"
	"context"
	"encoding/json"
	"errors"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

const AmountEntries = 1000000
const AmountMessageToSend = 10

// the number of function's doEntries calls (sand values to DB)
var i int = 0

func main() {
	ctx := context.Background()
	router := mux.NewRouter()
	srv := &http.Server{
		Addr:              `0.0.0.0:8080`,
		ReadTimeout:       time.Millisecond * 200,
		WriteTimeout:      time.Millisecond * 200,
		IdleTimeout:       time.Second * 10,
		ReadHeaderTimeout: time.Millisecond * 200,
		Handler:           router,
	}
	router.HandleFunc("/", handleClientRequest)
	go func() {
		log.Println(`Web Server started`)
		err := srv.ListenAndServe()
		if !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()

	consumeData()

	done := make(chan os.Signal, 1)
	signal.Notify(done,
		syscall.SIGTERM,
		syscall.SIGINT,
	)
	<-done
	log.Println(`Web Server is shutting down`)
	if err := srv.Shutdown(ctx); err != nil {
		log.Fatal(ctx, err)
	}
}

func handleClientRequest(rw http.ResponseWriter, _ *http.Request) {
	resp := make(map[string]string)
	amount, err := countRows()
	if err != nil {
		log.Println(err)
	}
	if i == 0 {
		err = produceData()
		if err != nil {
			log.Println(err)
		}
		resp["message"] = `SUCCESS`
		jsonResp, err := json.Marshal(resp)
		if err != nil {
			log.Println("Error happened in JSON marshal")
		}
		rw.Write(jsonResp)
		return
	} else {
		if amount >= AmountEntries {
			resp["message"] = `All entries was created successfully`
			jsonResp, err := json.Marshal(resp)
			if err != nil {
				log.Println("Error happened in JSON marshal")
			}
			rw.Write(jsonResp)
			return
		}

		resp["message"] = `Please wait. Was created only ` + strconv.Itoa(amount) + ` entries`
		jsonResp, err := json.Marshal(resp)
		if err != nil {
			log.Println("Error happened in JSON marshal")
		}
		rw.Write(jsonResp)
		return
	}
}

func produceData() error {
	pp := produce.NewBroker(context.Background(),
		`main`,
		`amqp://localhost`,
		`guest`,
		`guest`)
	s := produce.NewSlave(context.Background(), pp)
	for i := 0; i < AmountMessageToSend; i++ {
		err := s.Push(context.Background(), `key-product`, []byte(`{"someFields": "someValue 1"}`))
		if err != nil {
			log.Println(err)
		}
	}
	return nil
}

func consumeData() {
	{
		c := consume.NewMaster(context.Background(),
			`first`,
			`amqp://localhost`,
			`guest`,
			`guest`,
		)

		cS := consume.NewSlave(context.Background(), c, 1)
		err := cS.InitStream(context.Background())
		if err != nil {
			log.Println(err)
		}

		for {
			for {
				if !cS.IsDeliveryReady {
					log.Println(`Waiting...`)
					time.Sleep(consume.ReconnectDelay)
				} else {
					break
				}
			}

			select {
			case <-cS.Closed():
				continue // to get new stream in select/case
			case d := <-cS.GetStream():
				res := make(map[string]any)
				if err := json.Unmarshal(d.Body, &res); err != nil {
					log.Println(err)
				}
				log.Println(res)
				i++
				err = doEntries(i)
				if err != nil {
					log.Println(err)
				}
				if err := d.Ack(false); err != nil {
					log.Println(err)
				}
			}
		}
	}
}
