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
	Describe("SaveLog", func() {
		AfterEach(func() {
			// empty the database
			mongoSession.DB("tasks_log").C("tasks_log").Remove(bson.M{})
			r.Table("tasks").Delete().Run(rethinkSession)
		})
		Context("save tasks with status_id equal to 2", func() {
			Context("there are several tasks with status_id equal to 2", func() {
				It("should save a log with that number of tasks per city and the current datetime", func() {
					var mongoDb = mongoSession.DB("tasks_log")
					var taskLogger = logger.NewTaskLogger(rethinkSession, mongoDb)
					var c = mongoDb.C("tasks_log")

					r.Table("tasks").Insert([]map[string]interface{}{
						{
							"status_id": 2, // waiting
							"city_id":   1, // city 1
						},
						{
							"status_id": 2, // waiting
							"city_id":   1, // city 1
						},
						{
							"status_id": 5, // finished
							"city_id":   2, // city 2
						},
						{
							"status_id": 2, // waiting
							"city_id":   3, // city 3
						},
					}).Run(rethinkSession)

					taskLogger.SaveLog()

					log := logger.Log{}

					c.Find(bson.M{}).One(&log)

					Expect(log.Date.IsZero()).To(BeFalse())

					city1 := logger.TasksByCity{CityID: 1, Count: 2}
					city3 := logger.TasksByCity{CityID: 3, Count: 1}

					Expect(log.WaitingTasks).To(ConsistOf(city1, city3))

				})
			})
			Context("there are no tasks with status_id equal to 2", func() {
				It("should save a log with zero tasks and the current datetime", func() {
					var mongoDb = mongoSession.DB("tasks_log")
					var taskLogger = logger.NewTaskLogger(rethinkSession, mongoDb)
					var c = mongoDb.C("tasks_log")

					taskLogger.SaveLog()

					log := logger.Log{}

					c.Find(bson.M{}).One(&log)

					Expect(log.Date.IsZero()).To(BeFalse())
					Expect(len(log.WaitingTasks)).To(Equal(0))
				})
			})
		})
		Context("save tasks with status_id equal to 3 and 4", func() {
			Context("there are several tasks with status_id equal to 3 and 4", func() {
				It("should save a log with that number of tasks per city and the current datetime", func() {
					var mongoDb = mongoSession.DB("tasks_log")
					var taskLogger = logger.NewTaskLogger(rethinkSession, mongoDb)
					var c = mongoDb.C("tasks_log")

					r.Table("tasks").Insert([]map[string]interface{}{
						{
							"status_id": 3, // waiting
							"city_id":   1, // city 1
						},
						{
							"status_id": 4, // waiting
							"city_id":   1, // city 1
						},
						{
							"status_id": 2, // finished
							"city_id":   2, // city 2
						},
						{
							"status_id": 4, // waiting
							"city_id":   3, // city 3
						},
					}).Run(rethinkSession)

					taskLogger.SaveLog()

					log := logger.Log{}

					c.Find(bson.M{}).One(&log)

					Expect(log.Date.IsZero()).To(BeFalse())

					city1 := logger.TasksByCity{CityID: 1, Count: 2}
					city3 := logger.TasksByCity{CityID: 3, Count: 1}

					Expect(log.ActiveTasks).To(ConsistOf(city1, city3))

				})
			})
			Context("there are no tasks with status_id equal to 3 or 4", func() {
				It("should save a log with zero tasks and the current datetime", func() {
					var mongoDb = mongoSession.DB("tasks_log")
					var taskLogger = logger.NewTaskLogger(rethinkSession, mongoDb)
					var c = mongoDb.C("tasks_log")

					taskLogger.SaveLog()

					log := logger.Log{}

					c.Find(bson.M{}).One(&log)

					Expect(log.Date.IsZero()).To(BeFalse())
					Expect(len(log.ActiveTasks)).To(Equal(0))
				})
			})
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
