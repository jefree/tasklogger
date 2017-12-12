package connection

import (
	"fmt"
	"net/http"
	"tasklogger/logticker"

	"github.com/gorilla/websocket"
)

type Response struct {
	Action string      `json:"action"`
	Data   interface{} `json:"data,omitempty"`
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func HandleNewClient(w http.ResponseWriter, r *http.Request) {
	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer c.Close()

	logChan, logDone := logticker.LogBroadcaster.Listen().ReadChan()
	defer closeBroadcast(logChan, logDone)

	pingChan, pingDone := logticker.PingBroadcaster.Listen().ReadChan()
	defer closeBroadcast(pingChan, pingDone)

	for {
		select {
		case log := <-logChan:
			if err := c.WriteJSON(Response{Action: "update", Data: log}); err != nil {
				return
			}
		case <-pingChan:
			if err := c.WriteJSON(Response{Action: "ping"}); err != nil {
				return
			}
			fmt.Println("pinging")
		}
	}
}

func closeBroadcast(ch <-chan interface{}, done chan<- struct{}) {
	close(done)
	<-ch
}
