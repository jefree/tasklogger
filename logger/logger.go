package logger

import (
	"fmt"
	"time"

	r "gopkg.in/gorethink/gorethink.v3"
	mgo "gopkg.in/mgo.v2"
)

type Log struct {
	Date   time.Time
	Cities []CityStats
}

type CityStats struct {
	CityID   int
	Tasks    TasksStats
	Couriers CouriersStats
}

type TasksStats struct {
	Waiting, Assigned, InProgress int
}

type CouriersStats struct {
	Working, Connected, ConnectedWorking int
}

type CountByCityStatus struct {
	CityStatus []int `gorethink:"group"`
	Count      int   `gorethink:"reduction"`
}

type TaskLogger struct {
	rethinkSession *r.Session
	mongoDb        *mgo.Database
}

func (logger TaskLogger) CreateLog() Log {
	log := Log{}

	log.Date = time.Now()
	log.Cities = []CityStats{}

	log.populateTasksStatsByCity(logger.getTasksByCityStatus())

	return log
}

func (logger TaskLogger) getTasksByCityStatus() []CountByCityStatus {
	var result []CountByCityStatus

	cursor, err := r.Table("tasks").GetAllByIndex("status_id", 2, 3, 4).Group("city_id", "status_id").Count().Run(logger.rethinkSession)
	if err != nil {
		panic(err)
	}

	cursor.All(&result)

	return result
}

func (log *Log) populateTasksStatsByCity(stats []CountByCityStatus) {
	for _, stat := range stats {
		cityID := stat.CityStatus[0]
		statusID := stat.CityStatus[1]

		tasksStats := &log.getOrCreateCityStatsByID(cityID).Tasks

		fmt.Println(stat.Count)

		switch statusID {
		case 2:
			tasksStats.Waiting = stat.Count
		case 3:
			tasksStats.Assigned = stat.Count
		case 4:
			tasksStats.InProgress = stat.Count
		}
	}
}

func (log *Log) getOrCreateCityStatsByID(cityID int) *CityStats {
	for i, cityStats := range log.Cities {
		if cityStats.CityID == cityID {
			return &log.Cities[i]
		}
	}

	var newCityStats CityStats
	newCityStats.CityID = cityID

	log.Cities = append(log.Cities, newCityStats)

	fmt.Printf("new %p\n", &log.Cities[len(log.Cities)-1])

	return &log.Cities[len(log.Cities)-1]
}

func NewTaskLogger(rethinkSession *r.Session, mongoDb *mgo.Database) TaskLogger {
	return TaskLogger{
		rethinkSession: rethinkSession,
		mongoDb:        mongoDb,
	}
}
