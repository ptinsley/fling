package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/hpcloud/tail"
	"google.golang.org/api/option"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

/*
Todo:
- identify logging library for log level setting capability
- lots of error handling
- config param defaults
- add signal handler support
*/

var (
	configFile = kingpin.Flag("config", "Configuration file").Required().Short('c').String()
)

//FlingConfig - top level structure of json config file
type FlingConfig struct {
	PubSubProject  string      `json:"pubsub_project"`
	PubSubTopic    string      `json:"pubsub_topic"`
	PubSubAuthFile string      `json:"pubsub_auth_file"`
	Files          []FlingFile `json:"files"`
}

//FlingFile - instance of a file to monitor / rotate (if defined)
type FlingFile struct {
	Path           string `json:"path"`
	IsJSON         bool   `json:"is_json"`
	RotateCommand  string `json:"rotate_command"`
	RotateInterval int    `json:"rotate_interval"`
}

func main() {
	//Parse command line params
	kingpin.Parse()

	var config, err = loadConfig(*configFile)
	if err != nil {
		fmt.Printf("Error loading config (%s)", err)
		os.Exit(-1)
	}

	//Setup a channel to funnel messages from the file readers to pub/sub
	messages := make(chan []byte, 1000)

	//setup handlers for all of the files to be watched
	for _, file := range config.Files {
		handleFile(file, messages)
	}

	go processMessages(
		messages,
		config.PubSubProject,
		config.PubSubTopic,
		config.PubSubAuthFile)

	select {} //Take a big nap FIXME: add signal handlers down the road
}

func loadConfig(path string) (FlingConfig, error) {
	var config FlingConfig
	configString, err := ioutil.ReadFile(path)

	if err != nil {
		return config, err
	}

	json.Unmarshal(configString, &config)

	return config, nil
}

func handleFile(file FlingFile, messages chan []byte) {
	go follow(file.Path, messages, file.IsJSON)
	fmt.Printf("%s, %s every %d\n", file.Path, file.RotateCommand, file.RotateInterval)
	if file.RotateInterval > 0 && file.RotateCommand != "" {
		go rotate(file.Path, file.RotateCommand, file.RotateInterval)
	}

}

func follow(path string, messages chan []byte, isJSON bool) {
	t, err := tail.TailFile(path, tail.Config{Follow: true, ReOpen: true, Poll: true})

	if err != nil {
		fmt.Println("it borked!")
	}

	for line := range t.Lines {
		var logEntry map[string]interface{}

		if isJSON {
			json.Unmarshal([]byte(line.Text), &logEntry)
		} else {
			logEntry = make(map[string]interface{})
			logEntry["message"] = line.Text
		}

		//FIXME: Inject other pertinent context info
		logEntry["fling.source"] = path

		//fmt.Printf("sending (%s) into channel\n", line.Text)
		eventJSON, err := json.Marshal(logEntry)
		if err != nil {

		}
		messages <- eventJSON
	}
}

func rotate(path string, command string, interval int) {
	for {
		fmt.Printf("sleeping %d seconds\n", interval)
		time.Sleep(time.Duration(interval) * time.Second)

		fmt.Printf("Renaming %s\n", path)
		err := os.Rename(path, path+".old")
		if err == nil {
			exec.Command("sh", "-c", command).Output()
		}

	}
}

func processMessages(
	messages chan []byte,
	pubSubProject string,
	pubSubTopic string,
	pubSubAuthFile string) {

	ctx := context.Background()
	pubSubClient, err := pubsub.NewClient(ctx, pubSubProject, option.WithServiceAccountFile(pubSubAuthFile))
	if err != nil {

	}

	topic := pubSubClient.Topic(pubSubTopic)
	defer topic.Stop()

	for {
		message := <-messages
		//fmt.Printf("received: %s\n", message)
		result := topic.Publish(ctx, &pubsub.Message{
			Data: message,
		})

		id, err := result.Get(ctx)
		if err != nil {
			fmt.Printf("error publishing %s\n", err)
		} else {
			fmt.Printf("published with ID: %s\n", id)
		}

	}
}
