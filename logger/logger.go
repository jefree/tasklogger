package logger

import (
	"time"

	r "gopkg.in/gorethink/gorethink.v3"
	mgo "gopkg.in/mgo.v2"
)

type Log struct {
	Date           time.Time
	WaitingTasks   []CountByCity `bson:"waiting_tasks"`
	ActiveTasks    []CountByCity `bson:"active_tasks"`
	ActiveCouriers []CountByCity `bson:"active_couriers"`
}

type CountByCity struct {
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

	log.WaitingTasks = logger.getWaitingTasks()
	log.ActiveTasks = logger.getActiveTasks()
	log.ActiveCouriers = logger.getActiveCouriers()

	err := logger.mongoDb.C("tasks_log").Insert(log)
	if err != nil {
		panic(err)
	}
}

func (logger TaskLogger) getWaitingTasks() []CountByCity {
	var waitingTasks []CountByCity

	cursor, err := r.Table("tasks").GetAllByIndex("status_id", 2).Group("city_id").Count().Run(logger.rethinkSession)
	if err != nil {
		panic(err)
	}

	cursor.All(&waitingTasks)

	return waitingTasks
}

func (logger TaskLogger) getActiveTasks() []CountByCity {
	var activeTasks []CountByCity

	cursor, err := r.Table("tasks").GetAllByIndex("status_id", 3, 4).Group("city_id").Count().Run(logger.rethinkSession)
	if err != nil {
		panic(err)
	}
	cursor.All(&activeTasks)

	return activeTasks
}

func (logger TaskLogger) getActiveCouriers() []CountByCity {
	var activeCouriers []CountByCity

	cursor, err := r.Table("couriers").
		Filter(r.Row.Field("active_task_delivery").Count().Gt(0).Or(r.Row.Field("active_tasks_express").Count().Gt(0))).
		Group("city_id").Count().
		Run(logger.rethinkSession)

	if err != nil {
		panic(err)
	}

	cursor.All(&activeCouriers)

	return activeCouriers
}

func NewTaskLogger(rethinkSession *r.Session, mongoDb *mgo.Database) TaskLogger {
	return TaskLogger{
		rethinkSession: rethinkSession,
		mongoDb:        mongoDb,
	}
}
