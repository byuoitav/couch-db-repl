package main

import (
	"log"
	"net/http"

	"github.com/byuoitav/couch-db-repl/replication"
)

func main() {
	resp, err := http.Get("http://localhost:5984/")
	if err != nil {
		log.Printf("%v", err.Error())
	} else {
		log.Printf("%v", resp.StatusCode)

	}
	replication.A()
	replication.Check()
}
