package logger_test

import (
	"tasklogger/logger"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	r "gopkg.in/gorethink/gorethink.v3"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var (
	rethinkSession *r.Session
	mongoSession   *mgo.Session
)

var _ = Describe("TaskLogger", func() {
	Describe("CreateLog", func() {
		AfterEach(func() {
			// empty mongo database
			mongoSession.DB("mu_test").C("couriers_log").RemoveAll(bson.M{})

			// empty rethink database
			r.Table("tasks").Delete().Run(rethinkSession)
			r.Table("couriers").Delete().Run(rethinkSession)
		})
		It("should return a log with the stats by city", func() {
			var mongoDb = mongoSession.DB("mu_test")
			var taskLogger = logger.NewTaskLogger(rethinkSession, mongoDb)
			var mongoCouriersLog = mongoDb.C("couriers_log")

			r.Table("tasks").Insert([]map[string]interface{}{
				{
					"id":        1,
					"status_id": 2, // waiting
					"city_id":   1, // city 1
				},
				{
					"id":        2,
					"status_id": 2, // waiting
					"city_id":   1, // city 1
				},
				{
					"id":                3,
					"status_id":         5, // finished
					"city_id":           2, // city 2
					"global_courier_id": 3,
				},
				{
					"id":        4,
					"status_id": 2, // waiting
					"city_id":   3, // city 3
				},
				{
					"id":                5,
					"status_id":         3, // assigned
					"city_id":           1, // city 1
					"global_courier_id": 1,
				},
				{
					"id":                6,
					"status_id":         4, // in progress
					"city_id":           1, // city 1
					"global_courier_id": 2,
				},
				{
					"id":                7,
					"status_id":         4, // in progress
					"city_id":           3, // city 3
					"global_courier_id": 4,
				},
			}).Run(rethinkSession)

			r.Table("couriers").Insert([]map[string]interface{}{
				{
					"id":                   1,
					"city_id":              1,
					"active_task_delivery": []int{5},
					"active_tasks_express": []int{},
				},
				{
					"id":                   2,
					"city_id":              1,
					"active_task_delivery": []int{6},
					"active_tasks_express": []int{},
				},
				{
					"id":                   3,
					"city_id":              2,
					"active_task_delivery": []int{},
					"active_tasks_express": []int{},
				},
				{
					"id":                   4,
					"city_id":              3,
					"active_task_delivery": []int{},
					"active_tasks_express": []int{7},
				},
				{
					"id":      5,
					"city_id": 4,
				},
			}).Run(rethinkSession)

			err := mongoCouriersLog.Insert(
				bson.M{
					"date": "2017-12-11 14:28",
					"couriers": []bson.M{
						{
							"id":         1,
							"task_count": 2,
							"city_id":    1,
						},
					},
				},
				bson.M{
					"date": "2017-12-11 14:29",
					"couriers": []bson.M{
						{
							"id":         1,
							"task_count": 1,
							"city_id":    1,
						},
						{
							"id":         4,
							"task_count": 0,
							"city_id":    4,
						},
					},
				},
			)

			if err != nil {
				panic(err)
			}

			city1 := logger.CityStats{
				CityID: 1,
				Tasks: logger.TasksStats{
					Waiting:    2,
					Assigned:   1,
					InProgress: 1,
				},
				Couriers: logger.CouriersStats{
					Working:          2,
					ConnectedWorking: 1,
					ConnectedFree:    0,
				},
			}

			city3 := logger.CityStats{
				CityID: 3,
				Tasks: logger.TasksStats{
					Waiting:    1,
					Assigned:   0,
					InProgress: 1,
				},
				Couriers: logger.CouriersStats{
					Working:          1,
					ConnectedWorking: 0,
					ConnectedFree:    0,
				},
			}

			city4 := logger.CityStats{
				CityID: 4,
				Tasks: logger.TasksStats{
					Waiting:    0,
					Assigned:   0,
					InProgress: 0,
				},
				Couriers: logger.CouriersStats{
					Working:          0,
					ConnectedWorking: 0,
					ConnectedFree:    1,
				},
			}

			log := taskLogger.CreateLog()

			Expect(log.Date.IsZero()).To(BeFalse())
			Expect(log.Cities).To(ConsistOf(city1, city3, city4))
		})
	})
})

var _ = BeforeSuite(func() {
	setupRethinkSession()
	setupMongoDb()
})

var _ = AfterSuite(func() {
	rethinkSession.Close()
	mongoSession.Close()
})

func setupMongoDb() {
	mongoSession, _ = mgo.Dial("localhost")
}

func setupRethinkSession() {
	rethinkSession, _ = r.Connect(r.ConnectOpts{
		Address:  "localhost",
		Database: "mu_test",
	})
}
