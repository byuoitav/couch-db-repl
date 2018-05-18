package replication

import (
	"sync"

	"github.com/byuoitav/common/nerr"
)

type Scheduler struct {
	ConfigChan chan DatabaseConfig
	CurDB      DatabaseConfig
}

func (s *Scheduler) Run(config DatabaseConfig, wg *sync.WaitGroup) *nerr.E {

}
