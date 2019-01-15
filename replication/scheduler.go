package replication

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/byuoitav/common/log"
	"github.com/byuoitav/common/nerr"
)

func RunRegular(config DatabaseConfig, configChannel chan DatabaseConfig, wg *sync.WaitGroup) *nerr.E {
	defer wg.Done()
	savedInterval := 0

	//there's a 10 second floor
	if config.Interval < 10 && !config.Continuous {
		log.L.Infof("Interval of %v is too low, moving to the 10 second minimum", config.Interval)
		config.Interval = 10
	}

	for {
		log.L.Debugf("Starting replication run for %v", config.Database)
		//reset retry
		retry := false
		if savedInterval != 0 {
			config.Interval = savedInterval
			savedInterval = 0
		}

		err := ScheduleReplication(config.Database, config.Continuous)

		if err != nil {
			if config.Interval == 0 {
				savedInterval = config.Interval
				config.Interval = 60
			}
			log.L.Error(err.Addf("Issue scheduling replication for %v. Will try again in %v seconds", config.Database, config.Interval))
			retry = true
		}

		log.L.Debugf("Done for %v. Will run again in %v seconds", config.Database, config.Interval)
		if config.Continuous && !retry {
			select {
			case newConf, closed := <-configChannel:
				config = newConf
				if !closed {
					log.L.Warnf("Replication for %v is ending", config.Database)
					deleteReplication(fmt.Sprintf("auto_%v", config.Database))

					return nil
				}
			}
		} else {
			//start a timer
			t := time.NewTimer(time.Duration(config.Interval) * time.Second)
			select {
			case newConf, closed := <-configChannel:
				if !closed {
					log.L.Warnf("Replication for %v is ending", config.Database)
					deleteReplication(fmt.Sprintf("auto_%v", config.Database))

					return nil
				}
				config = newConf
				break
			case <-t.C:
				break
			}
		}
	}
	return nil
}

func RunConfig(curConfig HostConfig, config DatabaseConfig, wg *sync.WaitGroup, configChannels map[string]chan DatabaseConfig) *nerr.E {

	log.L.Infof("Running config for %s", config.Database)
	savedInterval := 0

	defer wg.Done()
	if config.Database != REPL_CONFIG_DB {
		return nerr.Create("Can't start a config DB not replicating the REPL_CONFIG_DB database", "invalid_args")
	}

	//there's a 10 second floor
	if config.Interval < 10 && !config.Continuous {
		log.L.Infof("Interval of %v is too low, moving to the 10 second minimum", config.Interval)
	}

	for {

		log.L.Debugf("Starting a run for %v", config.Database)
		if savedInterval != 0 {
			config.Interval = savedInterval
			savedInterval = 0
		}

		err := ScheduleReplication(REPL_CONFIG_DB, config.Continuous)
		if err != nil {
			if config.Interval == 0 {
				savedInterval = config.Interval
				config.Interval = 60
			}
			log.L.Error(err.Addf("Issue scheduling replication for %v. Will try again in %v seconds", config.Database, config.Interval))

			t := time.NewTimer(time.Duration(config.Interval) * time.Second)
			select {
			case <-t.C:
				continue
			}
		}
		//we need to get our configuration
		newGlobalConf, err := GetConfig(os.Getenv("PI_HOSTNAME"))

		if err != nil {
			//if this gets triggered it means someone deleted both the default and room specific configuration for this room.
			log.L.Errorf("Couldn't get the configuration for %v", os.Getenv("PI_HOSTNAME"))
			if config.Interval == 0 {
				savedInterval = config.Interval
				config.Interval = 60
			}
		} else {
			changes := true

			//we need to check if the configurations are equal
			if CheckHostConfigEquality(newGlobalConf, curConfig) {
				//we're the same
				changes = false
			}
			if changes == true {
				log.L.Debugf("%v === %v", newGlobalConf, curConfig)

				curConfig = newGlobalConf

				//this will redo everyone else
				UpdateConfigurations(curConfig, configChannels, wg)

				found := false
				//we need to go through and update our configuration
				for _, v := range curConfig.Replications {
					if v.Database == REPL_CONFIG_DB {
						config = v
						found = true
						break
					}
				}

				if found {
					continue
				}

				//we need to grab the default config
				config = DefaultReplConfig
			}
		}

		//start a timer
		log.L.Debugf("Done for %v. Will run again in %v seconds", config.Database, config.Interval)

		t := time.NewTimer(time.Duration(config.Interval) * time.Second)
		select {
		case <-t.C:
			break
		}
	}
}

func UpdateConfigurations(config HostConfig, channels map[string]chan DatabaseConfig, wg *sync.WaitGroup) {
	log.L.Infof("Updating configurations")
	valsInConfig := make(map[string]bool)
	for _, c := range config.Replications {
		if c.Database == REPL_CONFIG_DB {
			continue
		}
		valsInConfig[c.Database] = true

		//we go through and update/create as needed
		v, ok := channels[c.Database]
		if ok {
			v <- c
			continue
		} else {
			log.L.Infof("Creating replication job for %v", c.Database)
			//we need to create it
			wg.Add(1)

			//create a channel
			newChan := make(chan DatabaseConfig, 1)
			go RunRegular(c, newChan, wg)
			channels[c.Database] = newChan
		}

	}

	//now we need to go stop any replications that are no longer in the config
	for k, v := range channels {
		if _, ok := valsInConfig[k]; !ok {
			log.L.Infof("stoppping replication job for %v", k)
			close(v)
			delete(channels, k)
		}
	}

	//TODO: Should we delete the database when we stop replicating it? It'll get wiped when the container gets redeployed...
	log.L.Infof("Done.")
}

func StartReplicationJobs(config HostConfig) *nerr.E {

	wg := &sync.WaitGroup{}
	channelMap := make(map[string]chan DatabaseConfig)
	UpdateConfigurations(config, channelMap, wg)

	found := false
	for _, c := range config.Replications {
		if c.Database == REPL_CONFIG_DB {
			found = true
			go RunConfig(config, c, wg, channelMap)
		}
	}
	if !found {
		//run using the default
		go RunConfig(config, DefaultReplConfig, wg, channelMap)
	}

	wg.Wait()
	return nil
}
