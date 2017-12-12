package logger

import (
	"time"

	r "gopkg.in/gorethink/gorethink.v3"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type CityStats struct {
	CityID   int           `json:"city_id" bson:"city_id"`
	Tasks    TasksStats    `json:"tasks"`
	Couriers CouriersStats `json:"couriers"`
}

type TasksStats struct {
	Waiting    int `json:"waiting"`
	Assigned   int `json:"assigned"`
	InProgress int `json:"in_progress" bson:"in_progress"`
}

type CouriersStats struct {
	Working          int `json:"working"`
	ConnectedFree    int `json:"connected_free" bson:"connected_free"`
	ConnectedWorking int `json:"connected_working" bson:"connected_working"`
}

type countByCityStatus struct {
	CityStatus []int `gorethink:"group"`
	Count      int   `gorethink:"reduction"`
}

type workingCouriersByCity struct {
	CityID int `gorethink:"group"`
	Count  int `gorethink:"reduction"`
}

type connectedCourierStats struct {
	CityID    int `bson:"city_id"`
	TaskCount int `bson:"task_count"`
}

type couriersLog struct {
	Couriers []connectedCourierStats `bson:"couriers"`
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
	log.populateWorkingCouriersByCity(logger.getWorkingCouriersByCity())
	log.populateConnectedCouriersStatsByCity(logger.getConnectedCouriersStats())

	return log
}

func (logger TaskLogger) SaveLog(log Log) {
	err := logger.mongoDb.C("tasks_log").Insert(log)
	if err != nil {
		panic(err)
	}
}

func (logger TaskLogger) getTasksByCityStatus() []countByCityStatus {
	var result []countByCityStatus

	cursor, err := r.Table("tasks").GetAllByIndex("status_id", 2, 3, 4).Group("city_id", "status_id").Count().Run(logger.rethinkSession)
	if err != nil {
		panic(err)
	}

	cursor.All(&result)

	return result
}

func (logger TaskLogger) getConnectedCouriersStats() []connectedCourierStats {
	var couriersLog couriersLog

	err := logger.mongoDb.C("couriers_log").Find(bson.M{}).Sort("-date").Limit(1).One(&couriersLog)
	if err != nil {
		panic(err)
	}

	return couriersLog.Couriers
}

func (logger TaskLogger) getWorkingCouriersByCity() []workingCouriersByCity {
	var result []workingCouriersByCity

	cursor, err := r.Table("couriers").
		Filter(r.Row.Field("active_task_delivery").Count().Gt(0).
			Or(r.Row.Field("active_tasks_express").Count().Gt(0))).
		Group("city_id").
		Count().Run(logger.rethinkSession)

	if err != nil {
		panic(err)
	}

	cursor.All(&result)

	return result
}

type Log struct {
	Date   time.Time   `json:"date"`
	Cities []CityStats `json:"cities"`
}

func (log *Log) populateTasksStatsByCity(stats []countByCityStatus) {
	for _, stat := range stats {
		cityID := stat.CityStatus[0]
		statusID := stat.CityStatus[1]

		tasksStats := &log.getOrCreateCityStatsByID(cityID).Tasks

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

func (log *Log) populateWorkingCouriersByCity(workingStats []workingCouriersByCity) {
	for _, stat := range workingStats {
		courierStats := &log.getOrCreateCityStatsByID(stat.CityID).Couriers
		courierStats.Working = stat.Count
	}
}

func (log *Log) populateConnectedCouriersStatsByCity(connectedStats []connectedCourierStats) {
	couriersGroupByCity := groupCouriersByCity(connectedStats)

	for cityID, couriers := range couriersGroupByCity {
		courierStats := &log.getOrCreateCityStatsByID(cityID).Couriers

		for _, courier := range couriers {
			if courier.TaskCount > 0 {
				courierStats.ConnectedWorking++
			} else {
				courierStats.ConnectedFree++
			}
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

	return &log.Cities[len(log.Cities)-1]
}

func groupCouriersByCity(couriers []connectedCourierStats) map[int][]connectedCourierStats {
	var groupByCity = map[int][]connectedCourierStats{}

	for _, courier := range couriers {
		groupByCity[courier.CityID] = append(groupByCity[courier.CityID], courier)
	}

	return groupByCity
}

func NewTaskLogger(rethinkSession *r.Session, mongoDb *mgo.Database) TaskLogger {
	return TaskLogger{
		rethinkSession: rethinkSession,
		mongoDb:        mongoDb,
	}
}
