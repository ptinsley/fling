package main

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"os/exec"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/hpcloud/tail"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/option"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

/*
Todo:
- See if I can get hpcloud/tail to use logrus?
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

func init() {
	log.SetFormatter(&log.JSONFormatter{})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.WarnLevel)
}

func main() {
	log.Info("Initalizing")
	//Parse command line params
	kingpin.Parse()

	var config, err = loadConfig(*configFile)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Fatal("Couldn't load config")
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
	configString, readError := ioutil.ReadFile(path)

	if readError != nil {
		return config, readError
	}

	parseError := json.Unmarshal(configString, &config)

	return config, parseError
}

func handleFile(file FlingFile, messages chan []byte) {
	log.WithFields(log.Fields{
		"path": file.Path,
	}).Info("Adding tail for file")
	go follow(file.Path, messages, file.IsJSON)

	if file.RotateInterval > 0 && file.RotateCommand != "" {
		log.WithFields(log.Fields{
			"path": file.Path,
		}).Info("Rotate settings detected, adding rotate worker")
		go rotate(file.Path, file.RotateCommand, file.RotateInterval)
	}

}

func follow(path string, messages chan []byte, isJSON bool) {
	t, tailErr := tail.TailFile(path, tail.Config{Follow: true, ReOpen: true, Poll: true})

	if tailErr != nil {
		log.WithFields(log.Fields{
			"path":  path,
			"error": tailErr,
		}).Error("Couldn't tail file")
	}

	for line := range t.Lines {
		var logEntry map[string]interface{}

		log.WithFields(log.Fields{
			"path": path,
			"line": line.Text,
		}).Debug("Processing log line")

		if isJSON {
			unmarshalErr := json.Unmarshal([]byte(line.Text), &logEntry)
			if unmarshalErr != nil {
				log.WithFields(log.Fields{
					"message": line.Text,
					"error":   unmarshalErr,
				}).Error("Couldn't parse JSON log line")

				continue
			}
		} else {
			logEntry = make(map[string]interface{})
			logEntry["message"] = line.Text
		}

		//FIXME: Inject other pertinent context info
		logEntry["fling.source"] = path

		eventJSON, marshalErr := json.Marshal(logEntry)
		if marshalErr != nil {
			log.WithFields(log.Fields{
				"error": marshalErr,
			}).Error("Couldn't create JSON")

			continue
		}
		messages <- eventJSON
	}
}

func rotate(path string, command string, interval int) {
	for {
		log.WithFields(log.Fields{
			"seconds": interval,
			"path":    path,
		}).Info("Sleeping before rotate")

		time.Sleep(time.Duration(interval) * time.Second)

		renameErr := os.Rename(path, path+".old")
		if renameErr != nil {
			log.WithFields(log.Fields{
				"error": interval,
				"path":  path,
			}).Error("Unable to move log file in rotation")

			continue
		} else {
			log.WithFields(log.Fields{
				"path": path,
			}).Info("Moved log file")
		}

		_, cmdError := exec.Command("sh", "-c", command).Output()
		if cmdError != nil {
			log.WithFields(log.Fields{
				"error":   cmdError,
				"path":    path,
				"command": command,
			}).Error("Rotate command failed")
		} else {
			log.WithFields(log.Fields{
				"path":    path,
				"command": command,
			}).Info("Rotation command successful")
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
		result := topic.Publish(ctx, &pubsub.Message{
			Data: message,
		})

		id, err := result.Get(ctx)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
			}).Error("Failed to publish message")
		} else {
			log.WithFields(log.Fields{
				"id": id,
			}).Debug("Published Message")
		}

	}
}
