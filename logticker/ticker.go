package logticker

import (
	"tasklogger/logger"
	"time"

	"github.com/dagoof/gibb"
)

const (
	newLogPeriod = 1 * time.Minute
	pingPeriod   = 15 * time.Second
)

var (
	LogBroadcaster  = gibb.New()
	PingBroadcaster = gibb.New()
)

func RunLogTicker(taskLogger logger.TaskLogger) {
	var ticker = time.NewTicker(newLogPeriod)

	for _ = range ticker.C {
		log := taskLogger.CreateLog()
		taskLogger.SaveLog(log)

		LogBroadcaster.Write(log)
	}
}

func RunPingTicker() {
	var ticker = time.NewTicker(pingPeriod)

	for _ = range ticker.C {
		PingBroadcaster.Write(true)
	}
}
