package main

import (
	"net/http"
	"os"
	"syscall"

	"tasklogger/connection"
	"tasklogger/logger"
	"tasklogger/logticker"

	"github.com/spf13/viper"
	r "gopkg.in/gorethink/gorethink.v3"
	mgo "gopkg.in/mgo.v2"
)

type configuration struct {
	rethink struct {
		addresses []string
		database  string
	}

	mongo struct {
		host     string
		database string
		user     string
		pwd      string
		src      string
	}
}

func init() {
	setupFatalLog()
}

func main() {
	config := loadConfiguration()

	rethinkSession, err := r.Connect(r.ConnectOpts{
		Addresses: config.rethink.addresses,
		Database:  config.rethink.database,
	})
	if err != nil {
		panic("Error connecting to RethinkDB")
	}

	mongoSession, err := mgo.DialWithInfo(&mgo.DialInfo{
		Addrs:    []string{config.mongo.host},
		Username: config.mongo.user,
		Password: config.mongo.pwd,
	})
	if err != nil {
		panic("Error connecting to MongoDB")
	}

	taskLogger := logger.NewTaskLogger(rethinkSession, mongoSession.DB(config.mongo.database))

	go logticker.RunLogTicker(taskLogger)
	go logticker.RunPingTicker()

	http.HandleFunc("/task_log", connection.HandleNewClient)

	err = http.ListenAndServe(":8010", nil)
	if err != nil {
		panic(err)
	}
}

func loadConfiguration() configuration {
	appPath := os.Getenv("TASK_LOGGER_PATH")

	viper.SetConfigName("app")
	viper.AddConfigPath(appPath + "/config")

	if err := viper.ReadInConfig(); err != nil {
		panic("Error reading app configuration file")
	}

	config := configuration{}

	config.rethink.addresses = viper.GetStringSlice("rethink.addresses")
	config.rethink.database = viper.GetString("rethink.database")

	config.mongo.host = viper.GetString("mongo.host")
	config.mongo.database = viper.GetString("mongo.database")
	config.mongo.user = viper.GetString("mongo.user")
	config.mongo.pwd = viper.GetString("mongo.pwd")
	config.mongo.src = viper.GetString("mongo.src")

	return config
}

func setupFatalLog() {
	appPath := os.Getenv("TASK_LOGGER_PATH")

	fatalFile, fatalError := os.OpenFile(appPath+"/logs/fatal_log.txt", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if fatalError != nil {
		panic("Error abriendo el archivo fatal para el log: " + fatalError.Error())
	}

	if err := syscall.Dup2(int(fatalFile.Fd()), int(os.Stderr.Fd())); err != nil {
		panic("Falló al redireccionar los errores al archivo fatal: " + err.Error())
	}
}
