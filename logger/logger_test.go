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
			// empty mongo database
			mongoSession.DB("tasks_log").C("tasks_log").Remove(bson.M{})

			// empty rethink database
			r.Table("tasks").Delete().Run(rethinkSession)
			r.Table("couriers").Delete().Run(rethinkSession)
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

					city1 := logger.CountByCity{CityID: 1, Count: 2}
					city3 := logger.CountByCity{CityID: 3, Count: 1}

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

					city1 := logger.CountByCity{CityID: 1, Count: 2}
					city3 := logger.CountByCity{CityID: 3, Count: 1}

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
		Context("save the number of couriers with active tasks", func() {
			Context("there are several couriers with active  tasks", func() {
				It("should save a list with the number of couriers with active tasks on each city", func() {
					var mongoDb = mongoSession.DB("tasks_log")
					var taskLogger = logger.NewTaskLogger(rethinkSession, mongoDb)
					var c = mongoDb.C("tasks_log")

					r.Table("couriers").Insert([]map[string]interface{}{
						{
							"city_id":              1,
							"active_task_delivery": []int{1, 2},
							"active_tasks_express": []int{},
						},

						{
							"city_id":              1,
							"active_task_delivery": []int{1},
							"active_tasks_express": []int{2},
						},
						{
							"city_id":              2,
							"active_task_delivery": []int{},
							"active_tasks_express": []int{3, 4},
						},
						{
							"city_id":              3,
							"active_task_delivery": []int{},
							"active_tasks_express": []int{},
						},
						{
							"city_id":              4,
							"active_task_delivery": []int{5},
							"active_tasks_express": []int{6},
						},
						{
							"city_id": 5,
						},
					}).Run(rethinkSession)

					taskLogger.SaveLog()

					city1 := logger.CountByCity{CityID: 1, Count: 2}
					city2 := logger.CountByCity{CityID: 2, Count: 1}
					city4 := logger.CountByCity{CityID: 4, Count: 1}

					log := logger.Log{}
					c.Find(bson.M{}).One(&log)

					Expect(log.ActiveCouriers).To(ConsistOf(city1, city2, city4))

				})
			})
			Context("there are no couriers with active tasks", func() {
				It("should save an empty list for the couriers with active tasks", func() {
					var mongoDb = mongoSession.DB("tasks_log")
					var taskLogger = logger.NewTaskLogger(rethinkSession, mongoDb)
					var c = mongoDb.C("tasks_log")

					taskLogger.SaveLog()

					log := logger.Log{}
					c.Find(bson.M{}).One(&log)

					Expect(len(log.ActiveCouriers)).To(Equal(0))
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
