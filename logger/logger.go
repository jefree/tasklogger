package logger

import (
	"time"

	r "gopkg.in/gorethink/gorethink.v3"
	mgo "gopkg.in/mgo.v2"
)

type Log struct {
	Date         time.Time
	WaitingTasks []TasksByCity `bson:"waiting_tasks"`
	ActiveTasks  []TasksByCity `bson:"active_tasks"`
}

type TasksByCity struct {
	CityID int `gorethink:"group" bson:"city_id"`
	Count  int `gorethink:"reduction" bson:"count"`
}

type TaskLogger struct {
	rethinkSession *r.Session
	mongoDb        *mgo.Database
}

func (logger TaskLogger) SaveLog() {
	var log Log
	log.Date = time.Now()

	// get waiting tasks
	cursor, err := r.Table("tasks").GetAllByIndex("status_id", 2).Group("city_id").Count().Run(logger.rethinkSession)
	if err != nil {
		panic(err)
	}
	cursor.All(&log.WaitingTasks)

	// get active tasks
	cursor, err = r.Table("tasks").GetAllByIndex("status_id", 3, 4).Group("city_id").Count().Run(logger.rethinkSession)
	if err != nil {
		panic(err)
	}
	cursor.All(&log.ActiveTasks)

	err = logger.mongoDb.C("tasks_log").Insert(log)
	if err != nil {
		panic(err)
	}
}

func NewTaskLogger(rethinkSession *r.Session, mongoDb *mgo.Database) TaskLogger {
	return TaskLogger{
		rethinkSession: rethinkSession,
		mongoDb:        mongoDb,
	}
}
