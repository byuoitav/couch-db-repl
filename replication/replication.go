package replication

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/byuoitav/common/db/couch"
	l "github.com/byuoitav/common/log"
	"github.com/byuoitav/common/nerr"
)

type couchReplicationState struct {
	Database    string    `json:"database"`
	DocID       string    `json:"doc_id"`
	ID          string    `json:"id"`
	Source      string    `json:"source"`
	Target      string    `json:"target"`
	State       string    `json:"state"`
	ErrorCount  int       `json:"error_count"`
	StartTime   time.Time `json:"start_time"`
	LastUpdated time.Time `json:"last_updated"`
}

type couchReplicationPayload struct {
	ID           string      `json:"_id"`
	Rev          string      `json:"_rev,omitempty"`
	Source       string      `json:"source"`
	Target       string      `json:"target"`
	CreateTarget bool        `json:"create_target"`
	Continuous   bool        `json:"continous"`
	Selector     interface{} `json:"selector,omitempty"`
	Filter       string      `json:"filter,omitempty"`
}

type idSelector struct {
	ID regexQuery `json:"_id"`
}

type regexQuery struct {
	Regex string `json:"$regex"`
}

var PI_HOSTNAME = os.Getenv("SYSTEM_ID")
var COUCH_ADDR = os.Getenv("COUCH_ADDR")
var COUCH_USER = os.Getenv("COUCH_USER")
var COUCH_PASS = os.Getenv("COUCH_PASS")
var COUCH_REPL_ADDR = os.Getenv("COUCH_REPL_ADDR")
var COUCH_REPL_USER = os.Getenv("COUCH_REPL_USER")
var COUCH_REPL_PASS = os.Getenv("COUCH_REPL_PASS")

func ReplicateNow() *nerr.E {

	if len(os.Getenv("STOP_REPLICATION")) > 0 {
		l.L.Infof("Not replicating due to the stop replication env variable")
		return nil
	}

	//Config database is there. Check for a document for this room, if none, get the default
	config, err := GetConfig(PI_HOSTNAME)
	if err != nil {
		return err.Add("Error getting the replication config, could not start immediate replication.")
	}

	//we have the config - we can go ahead and schedule the updates
	for i := range config.Replications {
		ScheduleReplication(config.Replications[i].Database, false) // nolint:errcheck
	}

	return nil
}

func postReplication(repl couchReplicationPayload) *nerr.E {
	l.L.Debugf("Posting replication of %v", repl.ID)

	b, err := json.Marshal(repl)
	if err != nil {
		l.L.Debug("Couldn't marshal")
		return nerr.Translate(err).Addf("Couldn't marshal payload to start replication for %v", repl.ID)
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("%v/_replicator", COUCH_ADDR), bytes.NewReader(b))
	if err != nil {
		return nerr.Translate(err).Addf("Couldn't create request to start replication of %v", repl.ID)
	}

	req.Header.Add("Content-Type", "application/json")

	req.SetBasicAuth(COUCH_USER, COUCH_PASS)
	c := http.Client{}
	resp, err := c.Do(req)
	if err != nil {
		return nerr.Translate(err).Addf("Couldn't make request to start replication of %v", repl.ID)
	}

	defer resp.Body.Close()
	if resp.StatusCode/100 == 2 {
		l.L.Debugf("Replication for %v scheduled.", repl.ID)
		return nil
	}
	if resp.StatusCode == 409 {
		return nerr.Create("Conflict while creating the replication.", "conflict")
	}

	b, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return nerr.Translate(err).Addf("Couldn't read error response from couch server while creating replication %v", repl.ID)
	}

	ce := couch.CouchError{}
	err = json.Unmarshal(b, &ce)
	if err != nil {
		return nerr.Translate(err).Addf("Couldn't Unmarshal response from couch server while creating replication %v", repl.ID)
	}
	err = couch.CheckCouchErrors(ce)

	return nerr.Translate(err).Addf("Couldn't post replication document")
}

func ScheduleReplication(db string, continuous bool) *nerr.E {
	replID := fmt.Sprintf("auto_%v", db)

	//check to see if a replication for this database is already running. If so. check the state.
	status, err := CheckReplication(replID)

	if err != nil {
		return err.Addf("Couldn't schedule replication of %v", db)
	}

	if status == "running" || status == "started" || status == "added" {
		return nerr.Create(fmt.Sprintf("Replication for %v running. In state %v.", db, status), "duplicate_repl")
	}

	l.L.Debugf("Replication state: %v", status)

	//check to make sure the filter is there
	//filterName := fmt.Sprintf("filters/%v", replID)
	//filterName := "filters/deletedfilter"
	//filterName := ""
	/*
		err = CheckForReplicationFilter(db)

		if err == nil {
			l.L.Debugf("Error when checking for filter for %v", db)
			filterName = ""
		}*/

	//we can create the replication
	rdoc := couchReplicationPayload{
		ID:           replID,
		Source:       fmt.Sprintf("%v/%v", insertReplCreds(COUCH_REPL_ADDR), db),
		Target:       fmt.Sprintf("%v/%v", insertLocalCreds(COUCH_ADDR), db),
		CreateTarget: true,
		Continuous:   continuous,
		//Filter:       filterName,
	}

	// Filter devices table for only room specific devices
	if db == "devices" {
		pieces := strings.Split(PI_HOSTNAME, "-")
		rdoc.Selector = idSelector{
			ID: regexQuery{
				Regex: fmt.Sprintf("%s-%s-", pieces[0], pieces[1]),
			},
		}
	}

	err = postReplication(rdoc)
	if err == nil {
		l.L.Debugf("Replication for %v started successfully", db)
		return nil
	}
	switch err.Type {
	case "conflict":
		//We delete the document, and try again
		err = deleteReplication(replID)
		if err != nil {
			return err.Addf("Couldn't delete the replication document to schedule a new replication for %v", db)
		}
		err = postReplication(rdoc)
		if err != nil {
			return err.Addf("Schedling replication for %v after deleting old replication failed", db)
		}
	default:
		return err.Addf("Couldn't schedule replication for datbase: %v", db)
	}
	return nil

}

/*
func CheckForReplicationFilter(db string) *nerr.E {
	l.L.Debugf("Checking to see if replication filter for %v exists", db)

	headers := map[string]string{
		"Authorization": "Basic " + jsonhttp.BasicAuth(COUCH_USER, COUCH_PASS),
	}

	responseBody, response, responseErr := jsonhttp.CreateAndExecuteJSONRequest("Check Existence of Filter", "GET", fmt.Sprintf("%v/%v/_design/filter", COUCH_ADDR, db), "", headers, 30, nil)

	if responseErr != nil {
		return nerr.Translate(responseErr).Addf("Couldn't check existence of filter for database %v", db)
	}

	l.L.Debugf("Response received from couch while checking filter: %s", responseBody)

	if response.StatusCode == 404 {
		//Check if its already there
		responseBody, response, responseErr = jsonhttp.CreateAndExecuteJSONRequest("Check Existence of Database", "HEAD", fmt.Sprintf("%v/%v", COUCH_ADDR, db), "", headers, 30, nil)

		if responseErr != nil {
			return nerr.Translate(responseErr).Addf("Couldn't check existence of database %v", db)
		}

		if response.StatusCode/100 == 4 {
			//It isn't, create one
			responseBody, response, responseErr = jsonhttp.CreateAndExecuteJSONRequest("Create Database", "PUT", fmt.Sprintf("%v/%v", COUCH_ADDR, db), "", headers, 30, nil)

			if responseErr != nil {
				return nerr.Translate(responseErr).Addf("Couldn't create database %v", db)
			}

			l.L.Debugf("Response received from couch while create database: %s", responseBody)
		}

		//we need to go ahead and create one
		headers["Content-Type"] = "application/json"
		json := "{\"_id\": \"_design/filters\", \"filters\": { \"deletedfilter\": \"function(doc, req) { return !doc._deleted; };\" } }"
		responseBody, response, responseErr = jsonhttp.CreateAndExecuteJSONRequest("Create Filter Document", "PUT", fmt.Sprintf("%v/%v/_design/filter", COUCH_ADDR, db), json, headers, 30, nil)

		if responseErr != nil {
			return nerr.Translate(responseErr).Addf("Couldn't create filter document in db %v", db)
		}

		l.L.Debugf("Response received from couch while posting new filter: %s", responseBody)

		if response.StatusCode/100 != 2 {
			return nerr.Create(fmt.Sprintf("Non-200 response code when creating filter for %v", db), "response-received")
		}
	}

	return nil
}
*/

func CheckReplication(replID string) (string, *nerr.E) {
	l.L.Debugf("Checking to see if replication document %v is already scheduled", replID)

	req, err := http.NewRequest("GET", fmt.Sprintf("%v/_scheduler/docs/_replicator/%v", COUCH_ADDR, replID), nil)
	if err != nil {
		return "", nerr.Translate(err).Addf("Couldn't create request to check replication of %v", replID)
	}

	req.SetBasicAuth(COUCH_USER, COUCH_PASS)
	c := http.Client{}
	resp, err := c.Do(req)
	if err != nil {
		return "", nerr.Translate(err).Addf("Couldn't make request to check replication of %v", replID)
	}

	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", nerr.Translate(err).Addf("Couldn't read error response from couch server while checking for replication %v", replID)
	}

	if resp.StatusCode/100 != 2 {
		ce := couch.CouchError{}
		err = json.Unmarshal(b, &ce)
		if err != nil {
			return "", nerr.Translate(err).Addf("Couldn't Unmarshal response from couch server while checking for replication job %v", replID)
		}

		err = couch.CheckCouchErrors(ce)
		if _, ok := err.(*couch.NotFound); resp.StatusCode == 404 && ok {
			return "not_started", nil
		} else {
			return "", nerr.Translate(err).Addf("Issue checking replication status of %v", replID)
		}
	}
	//if it's a 200 response, lets see what the state is
	state := couchReplicationState{}
	err = json.Unmarshal(b, &state)
	if err != nil {
		return "", nerr.Translate(err).Addf("Couldn't unmarshal the replication state of %v", replID)
	}
	switch state.State {
	case "completed":
		return "completed", nil
	case "running":
		return "running", nil
	case "started":
		return "started", nil
	case "added":
		return "added", nil
	case "failed":
		return "failed", nil
	case "crashed":
		return "crashed", nil
	case "initializing":
		return "initializing", nil
	default:
		l.L.Errorf("Replication state for %v is in a bad state %v", replID, state.State)
		return state.State, nerr.Create(fmt.Sprintf("Replication of %v is in state %v", replID, state.State), "couch-repl-error")
	}
}

func getReplication(id string) (couchReplicationPayload, *nerr.E) {
	toReturn := couchReplicationPayload{}
	l.L.Debugf("Getting replication %v", id)

	req, err := http.NewRequest("GET", fmt.Sprintf("%v/_replicator/%v", COUCH_ADDR, id), nil)
	if err != nil {
		return toReturn, nerr.Translate(err).Addf("Couldn't get replication %v", id)
	}

	req.SetBasicAuth(COUCH_USER, COUCH_PASS)
	c := http.Client{}
	resp, err := c.Do(req)
	if err != nil {
		return toReturn, nerr.Translate(err).Addf("Couldn't make request to get replication %v", id)
	}

	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return toReturn, nerr.Translate(err).Addf("Couldn't read response while getting the replication %v", id)
	}

	if resp.StatusCode/100 == 2 {
		err := json.Unmarshal(b, &toReturn)
		if err != nil {
			return toReturn, nerr.Translate(err).Add("Couldn't unmarshal replication into a replication")
		}
		return toReturn, nil
	}

	ce := couch.CouchError{}
	err = json.Unmarshal(b, &ce)
	if err != nil {
		return toReturn, nerr.Translate(err).Addf("Couldn't unmarshal response from couch server while checking for replication doc %v", id)
	}

	err = couch.CheckCouchErrors(ce)
	return toReturn, nerr.Translate(err).Addf("Couldn't get replication document")
}

func deleteReplication(id string) *nerr.E {
	l.L.Debugf("Deleting replication %v", id)

	repl, err := getReplication(id)
	if err != nil {
		return err.Addf("Couldn't delete replication %v", id)
	}

	req, rerr := http.NewRequest("DELETE", fmt.Sprintf("%v/_replicator/%v?rev=%v", COUCH_ADDR, id, repl.Rev), nil)
	if rerr != nil { //"real" error
		return nerr.Translate(rerr).Addf("Couldn't delete replication %v", id)
	}

	req.SetBasicAuth(COUCH_USER, COUCH_PASS)
	c := http.Client{}
	resp, rerr := c.Do(req)
	if rerr != nil {
		return nerr.Translate(rerr).Addf("Couldn't make request to delete replication %v", id)
	}

	defer resp.Body.Close()
	if resp.StatusCode/100 == 2 {
		l.L.Debugf("Replication %v deleted", id)
		return nil
	}
	return nerr.Create(fmt.Sprintf("Couldn't delete the replication %v. Status code: %v", id, resp.StatusCode), "failure")
}

func insertLocalCreds(address string) string {
	return insertCreds(address, COUCH_USER, COUCH_PASS)
}

func insertReplCreds(address string) string {
	return insertCreds(address, COUCH_REPL_USER, COUCH_REPL_PASS)
}

func insertCreds(address, user, pass string) string {
	//we need to parse off the http[s]://
	vals := strings.Split(address, "//")
	return fmt.Sprintf("%v//%v:%v@%v", vals[0], user, pass, strings.Trim(vals[1], "/"))

}
