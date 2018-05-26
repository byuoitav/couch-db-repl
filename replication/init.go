package replication

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/byuoitav/common/db/couch"
	l "github.com/byuoitav/common/log"
	"github.com/byuoitav/common/nerr"
)

var DefaultReplConfig DatabaseConfig

func init() {

	addr := os.Getenv("COUCH_ADDR")
	if len(addr) < 1 {
		l.L.Fatal("No COUCH_ADDR specified")
	}
	if len(os.Getenv("COUCH_REPL_ADDR")) < 1 || len(os.Getenv("COUCH_REPL_ADDR")) < 1 || len(os.Getenv("COUCH_REPL_ADDR")) < 1 {
		l.L.Fatal("Remote environment variables are not declared.")
	}

	l.L.Debugf("Checking to see if couch server is up at %v", addr)
	//wait until the couch server is running
	for {
		resp, err := http.Get("http://localhost:5984/")
		//resp, err := http.Get("http://www.google.com")
		if err != nil {
			l.L.Info("Waiting for Couch to start...")
			l.L.Debugf("Error: %v", err.Error())
			time.Sleep(3 * time.Second)
			continue
		} else {
			resp.Body.Close()
			l.L.Debug("Couch is up.")
			break
		}
	}

	//check to see if we need to create all the databases
	db := []string{
		"_global_changes",
		"_replicator",
		"_users",
	}

	for _, d := range db {
		err := CheckDB(d)
		if err != nil {
			if err.Type == "not_found" {
				err := CreateDB(d)
				if err != nil {
					l.L.Debugf("%s", err.Stack)
					l.L.Fatal(err.Addf("Couldn't initialize database %v", db))
				}
			} else {
				l.L.Debugf("%s", err.Stack)
				l.L.Fatal(err.Addf("Couldn't validate/create meta database %s", db))
				os.Exit(1)
			}
		}
	}

	//setup the default config

	DefaultReplConfig.Database = REPL_CONFIG_DB
	DefaultReplConfig.Continuous = false
	DefaultReplConfig.Interval = 300

}

const (
	WAIT_LIMIT = 60
)

//Start is the entry point, this pulls down the _replication-config database, then acts on it to schedule replications
//of all other databases applicable for this host
func Start() *nerr.E {
	l.L.Info("Starting replication scheduler")

	err := CheckDB(REPL_CONFIG_DB)

	//check to see if the configdb is already down
	if err != nil {
		err := ScheduleReplication(REPL_CONFIG_DB, false)
		if err != nil {
			l.L.Debugf("%s", err.Stack)
			l.L.Fatal(err.Add("_replication-config database isn't present and we can't start replication"))
		}

		tries := 0
		for {
			log.Printf("Waiting for replication to succeed")

			if tries >= WAIT_LIMIT {
				l.L.Fatal("Exceeded retry limit for pulling down the replication config database.")
			}
			//waiting for the config db to replicate down
			state, err := CheckReplication(REPL_CONFIG_DB)
			if err != nil {
				l.L.Debugf("%s", err.Stack)
				l.L.Fatal(err.Add("_replication-config database replication failed, cannot start replication"))
			}
			if state != "completed" {
				break
			}
			if state == "failed" {
				l.L.Fatal("Replication of replication config database has failed.")
			}
			time.Sleep(1 * time.Second)
			tries++
		}
	}

	//Config database is there. Check for a document for this room, if none, get the default
	config, err := GetConfig(os.Getenv("PI_HOSTNAME"))
	if err != nil {
		return err.Add("Error getting the replication config while starting")
	}

	//we have the config - we can go ahead and schedule the updates
	StartReplicationJobs(config)
	return nil
}

func CheckDB(db string) *nerr.E {
	l.L.Debugf("Checking for DB %v", db)

	req, err := http.NewRequest("GET", fmt.Sprintf("%v/%v", os.Getenv("COUCH_ADDR"), db), nil)
	if err != nil {
		return nerr.Translate(err).Add("Couldn't create request")
	}

	req.SetBasicAuth(os.Getenv("COUCH_USER"), os.Getenv("COUCH_PASS"))
	c := http.Client{}
	resp, err := c.Do(req)
	if err != nil {
		return nerr.Translate(err).Addf("Couldn't make request for database: %v", db)
	}

	//we're all good
	if resp.StatusCode/100 == 2 {
		l.L.Debug("Database Present. Returning")
		return nil
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nerr.Translate(err).Addf("Couldn't read error response from couch server while checking for DB %v", db)
	}
	defer resp.Body.Close()

	//we can unmarshal the coucherr
	ce := couch.CouchError{}
	err = json.Unmarshal(b, &ce)
	if err != nil {
		return nerr.Translate(err).Addf("Couldn't Unmarshal response from couch server while checking for DB %v", db)
	}

	err = couch.CheckCouchErrors(ce)
	if resp.StatusCode == 404 {
		return nerr.Translate(err).Addf("Error checking for DB %v", db).SetType("not_found")
	}

	return nerr.Translate(err).Addf("Error checking for DB %v", db)
}

func CreateDB(db string) *nerr.E {
	l.L.Debug("Creating DB %v", db)

	req, err := http.NewRequest("PUT", fmt.Sprintf("%v/%v", os.Getenv("COUCH_ADDR"), db), nil)
	if err != nil {
		return nerr.Translate(err).Add("Couldn't create request")
	}

	req.SetBasicAuth(os.Getenv("COUCH_USER"), os.Getenv("COUCH_PASS"))
	c := http.Client{}
	resp, err := c.Do(req)
	if err != nil {
		return nerr.Translate(err).Addf("Couldn't make request to create database: %v", db)
	}
	defer resp.Body.Close()

	//we're all good
	if resp.StatusCode/100 == 2 {
		l.L.Debug("Database Created. Returning.")
		return nil
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nerr.Translate(err).Addf("Couldn't read error response from couch server while creating DB %v", db)
	}

	//we can unmarshal the coucherr
	ce := couch.CouchError{}
	err = json.Unmarshal(b, &ce)
	if err != nil {
		return nerr.Translate(err).Addf("Couldn't Unmarshal response from couch server while creating DB %v", db)
	}

	err = couch.CheckCouchErrors(ce)
	return nerr.Translate(err).Addf("Error creating DB %v", db)
}

func Check() string {
	log.Printf("yo")
	return "yo"
}
