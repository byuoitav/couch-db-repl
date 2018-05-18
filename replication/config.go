package replication

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/byuoitav/common/db/couch"
	l "github.com/byuoitav/common/log"
	"github.com/byuoitav/common/nerr"
)

const (
	REPL_CONFIG_DB = "replication-config"
)

type ReplicationConfig struct {
	ID    string       `json:"_id"`
	Rev   string       `json:"_rev,omitempty"`
	Rules []HostConfig `json:"rules,omitempty"`
}

type HostConfig struct {
	Hostname     string           `json:"hostname"`
	Replications []DatabaseConfig `json:"replications,omitempty"`
}

type DatabaseConfig struct {
	Database   string `json:"database"`
	Continuous bool   `json:"continuous"`
	Interval   int    `json:"interval,omitempty"`
}

func GetConfig(hostname string) (HostConfig, *nerr.E) {
	toReturn := HostConfig{}

	l.L.Debugf("Looking for a config for %v", hostname)

	//get the room config
	splitver := strings.Split(hostname, "-")

	//check to see if the room has one
	config, err := GetConfigDoc(fmt.Sprintf("%v-%v", splitver[0], splitver[1]))
	if err != nil {
		if err.Type == "*couch.NotFound" {
			l.L.Debug("Couldn't get a room specific configuration, getting the default")
			//get the default
			config, err = GetConfigDoc("_default")
			if err != nil {
				return toReturn, nerr.Translate(err).Add("Couldn't get the default or room specific configuration")
			}
		}
		return toReturn, nerr.Translate(err).Add("Couldn't get the configuration")
	}
	//now we go through and check all of the rules one by one to see which matches my hostname
	for _, rule := range config.Rules {
		match, err := regexp.MatchString(rule.Hostname, hostname)
		if err != nil {
			return toReturn, nerr.Translate(err).Addf("Couldn't run regex %v to match hostname for config.", rule.Hostname)
		}
		if match {
			l.L.Debugf("Found rule.")
			return rule, nil
		}
	}
	return toReturn, nerr.Create(fmt.Sprintf("Couldn't match a rule for %v in the config %v", hostname, config.ID), "not-found")
}

func GetConfigDoc(id string) (ReplicationConfig, *nerr.E) {

	l.L.Debugf("Getting config document %v", id)

	toReturn := ReplicationConfig{}

	addr := fmt.Sprintf("%v/%v", REPL_CONFIG_DB, id)
	l.L.Debugf("Sending request to %v", addr)

	db := couch.NewDB(os.Getenv("COUCH_ADDR"), os.Getenv("COUCH_USER"), os.Getenv("COUCH_PASS"))
	err := db.MakeRequest("GET", fmt.Sprintf("%v/%v", REPL_CONFIG_DB, id), "application/json", []byte{}, &toReturn)
	if err != nil {
		return toReturn, nerr.Translate(err).Addf("Couldn't get the configuration document %v", id)
	}

	return toReturn, nil
}

func CheckHostConfigEquality(a, b HostConfig) bool {
	if a.Hostname != b.Hostname {
		return false
	}
	if len(a.Replications) != len(b.Replications) {
		return false
	}
	for i := range a.Replications {
		if !CheckDBConfigEquality(a.Replications[i], b.Replications[i]) {
			return false
		}
	}

	return true
}

func CheckDBConfigEquality(a, b DatabaseConfig) bool {
	if a.Database != b.Database {
		return false
	}
	if a.Interval != b.Interval {
		return false
	}
	return a.Continuous == b.Continuous
}
