package main

import (
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"gopkg.in/mgo.v2"

	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

type Configuration struct {
	MQQT    MQQTConfiguration
	MongoDB MongoDBConfiguration
}

type MQQTConfiguration struct {
	Uri      string
	Username string
	Password string
	ClientID string
}

type MongoDBConfiguration struct {
	ConnectionString string
	Database         string
}

type TrackboxEvent struct {
	Event            string    `bson:"Event"`
	EntityType       string    `bson:"EntityType"`
	EntityId         string    `bson:"EntityId"`
	TargetEntityType string    `bson:"TargetEntityType"`
	TargetEntityId   string    `bson:"TargetEntityId"`
	EventTime        time.Time `bson:"EventTime"`
}

type MQQTEvent struct {
	Type             string  `json:"_type"`
	WaypointCreation int64   `json:"wtst"`
	Latitude         float64 `json:"lat"`
	Longitude        float64 `json:"long"`
	Timestamp        int64   `json:"tst"`
	Accuracy         float64 `json:"acc"`
	TrackerId        string  `json:"tid"`
	Event            string  `json:"event"`
	Description      string  `json:"desc"`
	Trigger          string  `json:"t"`
}

var collection *mgo.Collection

var f MQTT.MessageHandler = func(client MQTT.Client, msg MQTT.Message) {
	log.Printf("TOPIC: %s\n", msg.Topic())
	log.Printf("MSG: %s\n", msg.Payload())
	if msg.Duplicate() {
		log.Printf("Ignoring duplicate message %s\n", msg.MessageID())
		return
	}

	var evt MQQTEvent
	err := json.Unmarshal(msg.Payload(), &evt)
	if err != nil {
		log.Println(err)
		return
	}

	if evt.Type != "transition" {
		return
	}

	username := strings.Split(msg.Topic(), "/")[1]

	trackboxEvent := TrackboxEvent{Event: evt.Event, EntityType: "user", EntityId: username, TargetEntityType: "geofence", TargetEntityId: evt.Description, EventTime: time.Unix(evt.Timestamp, 0)}
	err = collection.Insert(&trackboxEvent)
	if err != nil {
		log.Println(err)
	}
}

func main() {
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigs
		fmt.Println()
		fmt.Println(sig)
		done <- true
	}()

	configuration := readConfiguration()

	session, err := mgo.Dial(configuration.MongoDB.ConnectionString)
	if err != nil {
		log.Panic(err)
	}
	defer session.Close()
	collection = session.DB(configuration.MongoDB.Database).C("Event")

	opts := MQTT.NewClientOptions().AddBroker(configuration.MQQT.Uri)
	opts.SetUsername(configuration.MQQT.Username)
	opts.SetPassword(configuration.MQQT.Password)
	opts.SetClientID(configuration.MQQT.ClientID)
	opts.SetDefaultPublishHandler(f)

	c := MQTT.NewClient(opts)
	if token := c.Connect(); token.Wait() && token.Error() != nil {
		log.Panic(token.Error())
	}

	if token := c.Subscribe("owntracks/+/+/event", 2, nil); token.Wait() && token.Error() != nil {
		log.Panic(token.Error())
	}

	fmt.Println("awaiting signal")
	<-done
	fmt.Println("exiting")
	c.Disconnect(250)
}

func readConfiguration() Configuration {
	file, _ := os.Open("conf.json")
	decoder := json.NewDecoder(file)
	configuration := Configuration{}
	err := decoder.Decode(&configuration)
	if err != nil {
		log.Panic(err)
	}

	return configuration
}
