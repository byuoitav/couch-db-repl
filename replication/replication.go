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
}

func postReplication(repl couchReplicationPayload) *nerr.E {
	l.L.Debugf("Posting replication of %v", repl.ID)

	b, err := json.Marshal(repl)
	if err != nil {
		l.L.Debug("Couldn't marshal")
		return nerr.Translate(err).Addf("Couldn't marshal payload to start replication for %v", repl.ID)
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("%v/_replicator", os.Getenv("COUCH_ADDR")), bytes.NewReader(b))
	if err != nil {
		return nerr.Translate(err).Addf("Couldn't create request to start replication of %v", repl.ID)
	}

	req.Header.Add("Content-Type", "application/json")

	req.SetBasicAuth(os.Getenv("COUCH_USER"), os.Getenv("COUCH_PASS"))
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

func ScheduleReplication(db string) *nerr.E {
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

	//we can create the replication
	rdoc := couchReplicationPayload{
		ID:           replID,
		Source:       fmt.Sprintf("%v/%v", insertReplCreds(os.Getenv("COUCH_REPL_ADDR")), db),
		Target:       fmt.Sprintf("%v/%v", insertLocalCreds(os.Getenv("COUCH_ADDR")), db),
		CreateTarget: true,
		Continuous:   false,
	}

	err = postReplication(rdoc)
	if err == nil {
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

func CheckReplication(replID string) (string, *nerr.E) {
	l.L.Debugf("Checking to see if replication document %v is already scheduled", replID)

	req, err := http.NewRequest("GET", fmt.Sprintf("%v/_scheduler/docs/_replicator/%v", os.Getenv("COUCH_ADDR"), replID), nil)
	if err != nil {
		return "", nerr.Translate(err).Addf("Couldn't create request to check replication of %v", replID)
	}

	req.SetBasicAuth(os.Getenv("COUCH_USER"), os.Getenv("COUCH_PASS"))
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
	default:
		l.L.Errorf("Replication state for %v is in a bad state %v", replID, state.State)
		return state.State, nerr.Create(fmt.Sprintf("Replication of %v is in state %v", replID, state.State), "couch-repl-error")
	}
}

func getReplication(id string) (couchReplicationPayload, *nerr.E) {
	toReturn := couchReplicationPayload{}
	l.L.Debugf("Getting replication %v", id)

	req, err := http.NewRequest("GET", fmt.Sprintf("%v/_replicator/%v", os.Getenv("COUCH_ADDR"), id), nil)
	if err != nil {
		return toReturn, nerr.Translate(err).Addf("Couldn't get replication %v", id)
	}

	req.SetBasicAuth(os.Getenv("COUCH_USER"), os.Getenv("COUCH_PASS"))
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

	req, rerr := http.NewRequest("DELETE", fmt.Sprintf("%v/_replicator/%v?rev=%v", os.Getenv("COUCH_ADDR"), id, repl.Rev), nil)
	if rerr != nil { //"real" error
		return nerr.Translate(rerr).Addf("Couldn't delete replication %v", id)
	}

	req.SetBasicAuth(os.Getenv("COUCH_USER"), os.Getenv("COUCH_PASS"))
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
	return insertCreds(address, os.Getenv("COUCH_USER"), os.Getenv("COUCH_PASS"))
}

func insertReplCreds(address string) string {
	return insertCreds(address, os.Getenv("COUCH_REPL_USER"), os.Getenv("COUCH_REPL_PASS"))
}

func insertCreds(address, user, pass string) string {
	//we need to parse off the http[s]://
	vals := strings.Split(address, "//")
	return fmt.Sprintf("%v//%v:%v@%v", vals[0], user, pass, strings.Trim(vals[1], "/"))

}