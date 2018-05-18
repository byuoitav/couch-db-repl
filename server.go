package main

import (
	"github.com/byuoitav/common/log"
	"github.com/byuoitav/couch-db-repl/replication"
)

func main() {
	err := replication.Start()
	if err != nil {
		log.L.Error(err)

	}
}
