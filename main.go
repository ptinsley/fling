package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/hpcloud/tail"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/option"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	configFile  = kingpin.Flag("config", "Configuration file").Required().Short('c').String()
	debugFlag   = kingpin.Flag("debug", "Enable Debug Logging").Short('d').Bool()
	inotifyFlag = kingpin.Flag("inotify", "Enable iNotify file monitoring").Short('i').Bool()
	version     = "master" //overridden by build system, master as default
)

/*
Todo:
- See if I can get hpcloud/tail to use logrus?
- lots of error handling
- config param defaults
- add signal handler support
*/

//FlingEvent - struct to hold events and any further tracking fields needed
// (that won't follow the event out of fling)
type FlingEvent struct {
	UniqueID string
	JSON     map[string]interface{}
}

//FlingConfig - top level structure of json config file
type FlingConfig struct {
	Input     FlingInput      `json:"input"`
	Files     []FlingInFile   `json:"files"`
	Rotations []FlingRotation `json:"rotations"`
	Output    FlingOutput     `json:"output"`
}

//FlingInput - map of input type arrays
type FlingInput struct {
	PubSubs []FlingInPubSub `json:"pubsub"`
	Files   []FlingInFile   `json:"files"`
}

//FlingInPubSub - pub/sub input type
type FlingInPubSub struct {
	AuthFile     string           `json:"auth_file"`
	Project      string           `json:"project"`
	Subscription string           `json:"subscription"`
	Outputs      []string         `json:"outputs"`
	Injections   []FlingInjection `json:"injections"`
}

// FlingRotation - sets of files to rotate and the commands to run afterwards
type FlingRotation struct {
	Files          []string `json:"files"`
	RotateCommand  string   `json:"rotate_command"`
	RotateInterval int      `json:"rotate_interval"`
}

//FlingOutput - map of output types
type FlingOutput struct {
	PubSubs    []FlingOutPubSub   `json:"pubsub"`
	Loggers    []FlingOutLogger   `json:"logger"`
	Elastics   []FlingOutElastic  `json:"elasticsearch"`
	BigQueries []FlingOutBigQuery `json:"bigquery"`
}

//FlingOutBigQuery - Big query output config
type FlingOutBigQuery struct {
	Name         string `json:"name"`
	ProjectID    string `json:"project_id"`
	BatchSize    int    `json:"batch_size,omitempty"`
	BatchTimeout int    `json:"batch_timeout,omitempty"`
}

//FlingOutElastic - Elastic output config
type FlingOutElastic struct {
	Name     string               `json:"name"`
	Index    string               `json:"index_pattern"`
	Hosts    []string             `json:"hosts"`
	Template FlingElasticTemplate `json:"template"`
}

//FlingElasticTemplate - information on managing an elasticsearch indexing template
type FlingElasticTemplate struct {
	Name      string `json:"name"`
	Manage    bool   `json:"manage"`
	Overwrite bool   `json:"overwrite"`
	Path      string `json:"path"`
}

//FlingOutPubSub - A log output destination
type FlingOutPubSub struct {
	Name     string `json:"name"`
	Project  string `json:"project"`
	Topic    string `json:"topic"`
	AuthFile string `json:"auth_file"`
}

//FlingOutLogger - Send messages to the logger library
//FIXME add level and injections
type FlingOutLogger struct {
	Name      string `json:"name"`
	IsEnabled bool   `json:"is_enabled"`
}

//FlingInFile - instance of a file to monitor
type FlingInFile struct {
	Path         string           `json:"path"`
	IsJSON       bool             `json:"is_json"`
	IsGlob       bool             `json:"is_glob"`
	GlobInterval int              `json:"glob_interval"`
	Outputs      []string         `json:"outputs"`
	Injections   []FlingInjection `json:"injections"`
}

//FlingInjection - fields to add to the log line
type FlingInjection struct {
	Field    string `json:"field"`
	Value    string `json:"value"`
	ENVValue string `json:"env_value"`
	Hostname bool   `json:"hostname"`
}

func init() {
	log.SetFormatter(&log.JSONFormatter{})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)
}

func main() {
	log.Info("Initalizing")
	//Parse command line params
	kingpin.Version(version)
	kingpin.Parse()

	if *debugFlag {
		log.SetLevel(log.DebugLevel)
	}

	var config, err = loadConfig(*configFile)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Fatal("Couldn't load config")
		os.Exit(-1)
	}

	//var outputs map[string]interface{}
	//start up go routines for any outputs
	outputChannels := handleOutputs(config.Output)

	//start up go routines for any rotations requested
	handleRotations(config.Rotations)

	//setup handlers for all of the files to be watched
	handleInputs(config.Input, outputChannels)

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

func handleInputs(input FlingInput, outputChannels map[string]interface{}) {
	handleInFiles(input.Files, outputChannels)
	//handleInPubSub(input.PubSubs, outputChannels)
}

func handleOutputs(outputs FlingOutput) map[string]interface{} {
	var channels map[string]interface{}
	channels = make(map[string]interface{})

	for k, v := range handleOutPubSubs(outputs.PubSubs) {
		channels[k] = v
	}

	for k, v := range handleOutLoggers(outputs.Loggers) {
		channels[k] = v
	}
	/*
		for k, v := range handleOutElastics(outputs.Elastics) {
			channels[k] = v
		}

		for k, v := range handleOutBigQuery(outputs.BigQueries) {
			channels[k] = v
		}
	*/

	return channels
}

func handleOutBigQuery(outputs []FlingOutBigQuery) map[string]interface{} {
	var channels map[string]interface{}
	channels = make(map[string]interface{})

	for _, output := range outputs {
		if output.BatchSize == 0 {
			log.WithFields(log.Fields{
				"OutputName": output.Name,
			}).Debug("BigQuery BatchSize not set, applying default of 500")
			output.BatchSize = 500
		}
		if output.BatchTimeout == 0 {
			log.WithFields(log.Fields{
				"OutputName": output.Name,
			}).Debug("BigQuery BatchTimeout not set, applying default of 30 seconds")
			output.BatchTimeout = 30
		}
		//FIXME add error checking to projectID etc...
		channels[output.Name] = make(chan FlingEvent, 1000)
		go outputBigQueryWorker(output, channels[output.Name].(chan FlingEvent))
	}

	return channels
}

func outputBigQueryWorker(output FlingOutBigQuery, channel chan FlingEvent) {
	var batch []FlingEvent
	flush := make(chan bool, 1)

	var timeout = time.Duration(output.BatchTimeout) * time.Second
	timer := time.NewTimer(timeout)
	/*
		ctx := context.Background()

		client, err := bigquery.NewClient(ctx, output.ProjectID)
		if err != nil {
			log.WithFields(log.Fields{
				"OutputName": output.Name,
			}).Fatal(fmt.Sprintf("Failed to create client: %v", err))

			return
		}
	*/

	for {
		select {
		case event := <-channel:
			log.WithFields(log.Fields{
				"OutputName": output.Name,
				"UniqueID":   event.UniqueID,
			}).Debug(fmt.Sprintf("outputBigQueryWorker appending %s", event.JSON))

			batch = append(batch, event)

			if len(batch) == output.BatchSize {
				log.WithFields(log.Fields{
					"OutputName": output.Name,
				}).Debug("batch size reached, flushing")
				flush <- true
			}
		case <-flush:
			if len(batch) > 0 {
				log.WithFields(log.Fields{
					"OutputName": output.Name,
				}).Debug("batch complete")

				//FIXME: Add the code to actually send stuff to BQ

				batch = nil
				timer.Reset(timeout)
			}
		case <-timer.C:
			log.WithFields(log.Fields{
				"OutputName": output.Name,
			}).Debug("timer exceeded")

			flush <- true
		}
	}
}

func handleOutLoggers(outputs []FlingOutLogger) map[string]interface{} {
	var channels map[string]interface{}
	channels = make(map[string]interface{})

	for _, output := range outputs {
		channels[output.Name] = make(chan FlingEvent, 1000)
		go outputLoggerWorker(output.Name, output.IsEnabled, channels[output.Name].(chan FlingEvent))
	}

	return channels
}

func outputLoggerWorker(name string, isEnabled bool, channel chan FlingEvent) {
	for {
		event := <-channel

		if isEnabled {
			log.WithFields(log.Fields{
				"OutputName": name,
				"UniqueID":   event.UniqueID,
			}).Debug(fmt.Sprintf("%s", event.JSON))
		}
	}
}

func handleOutPubSubs(outputs []FlingOutPubSub) map[string]interface{} {
	var channels map[string]interface{}
	channels = make(map[string]interface{})

	for _, output := range outputs {
		channels[output.Name] = make(chan FlingEvent, 1000)
		go pubSubOutWorker(output.Project, output.Topic, output.AuthFile, channels[output.Name].(chan FlingEvent))
	}

	return channels
}

func pubSubOutWorker(project string, topicName string, authfile string, channel chan FlingEvent) {
	if project == "" {
		log.Fatal("PubSub output project must be defined")
		panic("Invalid Config")
	}
	if topicName == "" {
		log.Fatal("PubSub output topic must be defined")
		panic("Invalid Config")
	}
	if authfile == "" {
		log.Fatal("PubSub output authfile must be defined")
		panic("Invalid Config")
	}

	ctx := context.Background()
	pubSubClient, err := pubsub.NewClient(ctx, project, option.WithServiceAccountFile(authfile))
	if err != nil {
		log.WithFields(log.Fields{
			"project":   project,
			"topic":     topicName,
			"auth_file": authfile,
		}).Fatal(fmt.Sprintf("Could not create pubsub Client: %v", err))
		panic("Couldn't authenticate to pubsub")
	}

	topic := pubSubClient.Topic(topicName)
	defer topic.Stop()

	//send hello message to topic to keep track of what clients, versions etc.. are sending in data
	// Run this in a go routine to avoid a race condition with inputs filling the channel
	go createPubSubInitMsg(topicName, channel)

	for {
		event := <-channel

		message, marshalErr := json.Marshal(event.JSON)
		if marshalErr != nil {
			log.Error("Event Marshalling for pub/sub submission failed")
			return
		}
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

func handleOutElastics(outputs []FlingOutElastic) map[string]interface{} {
	var channels map[string]interface{}
	channels = make(map[string]interface{})

	for _, output := range outputs {
		channels[output.Name] = make(chan FlingEvent, 1000)
		go elasticOutWorker(output, channels[output.Name].(chan FlingEvent))
	}

	return channels
}

func elasticOutWorker(config FlingOutElastic, channel chan FlingEvent) {
	if config.Template != (FlingElasticTemplate{}) {
		log.WithFields(log.Fields{}).Debug("handling elastic template")
		handleElasticTemplate(config)
	}

	//logstash-%{+YYYY.MM.dd}

}

func handleElasticTemplate(config FlingOutElastic) {
	//FIXME do the thing

}

func createPubSubInitMsg(topicName string, channel chan FlingEvent) {
	var logEntry map[string]interface{}
	logEntry = make(map[string]interface{})
	hostname, _ := os.Hostname()

	logEntry["pubsub_topic"] = topicName
	logEntry["fling_version"] = version
	logEntry["hostname"] = hostname
	logEntry["@timestamp"] = get3339Time()
	logEntry["message"] = "Starting up Fling PubSub Output"

	channel <- FlingEvent{JSON: logEntry}
	log.WithFields(log.Fields{"topic": topicName}).Info("PubSub Init message queued")
}

func handleInFiles(files []FlingInFile, outputs map[string]interface{}) {
	for _, file := range files {
		if file.IsGlob {
			go fileInGlobWatcher(file, outputs)

		} else {
			startFileWorker(file, outputs)
		}
	}
}

func fileInGlobWatcher(file FlingInFile, outputs map[string]interface{}) {
	if file.GlobInterval == 0 {
		file.GlobInterval = 30
	}
	watchedPaths := make(map[string]bool)
	var globPattern = file.Path

	for {
		log.WithFields(log.Fields{
			"path": globPattern,
		}).Debug("Checking for new files in glob")

		paths, _ := filepath.Glob(globPattern)
		for _, path := range paths {
			file.Path = path

			if _, exists := watchedPaths[file.Path]; !exists {
				watchedPaths[file.Path] = true
				startFileWorker(file, outputs)
			}
		}

		time.Sleep(time.Duration(file.GlobInterval) * time.Second)
	}

}

func startFileWorker(file FlingInFile, outputs map[string]interface{}) {
	log.WithFields(log.Fields{
		"path": file.Path,
	}).Info("Adding tail for file")
	go fileInWorker(file, outputs)
}

func fileInWorker(file FlingInFile, outputs map[string]interface{}) {
	for {

		t, tailErr := tail.TailFile(file.Path, tail.Config{Follow: true, ReOpen: true, Poll: !*inotifyFlag, Location: &tail.SeekInfo{0, os.SEEK_END}})

		if tailErr != nil {
			log.WithFields(log.Fields{
				"path":  file.Path,
				"error": tailErr,
			}).Error("Couldn't tail file")
		} else {
			log.Info("tailed log")
		}

	Processing:
		for {
			select {
			case line := <-t.Lines:
				processInFileLine(line, file, outputs)
			case <-time.After(time.Hour):
				t.Stop()
				break Processing
			}
		}
	}
}

func processInFileLine(line *tail.Line, file FlingInFile, outputs map[string]interface{}) {
	var logEntry map[string]interface{}

	log.WithFields(log.Fields{
		"path": file.Path,
		"line": line.Text,
	}).Debug("Processing log line")

	if file.IsJSON {
		unmarshalErr := json.Unmarshal([]byte(line.Text), &logEntry)
		if unmarshalErr != nil {
			log.WithFields(log.Fields{
				"message": line.Text,
				"error":   unmarshalErr,
			}).Error("Couldn't parse JSON log line")

			return
		}
	} else {
		logEntry = make(map[string]interface{})
		logEntry["message"] = line.Text
	}

	//FIXME: Inject other pertinent context info
	logEntry["fling.source"] = file.Path

	if _, ok := logEntry["@timestamp"]; !ok {
		logEntry["@timestamp"] = get3339Time()
	}

	handleInjections(&logEntry, file.Injections)

	dispatchEntry(FlingEvent{UniqueID: "", JSON: logEntry}, file.Outputs, outputs)
}

func handleInjections(logEntry *map[string]interface{}, injections []FlingInjection) {
	for _, injection := range injections {
		if injection.ENVValue != "" {
			(*logEntry)[injection.Field] = os.Getenv(injection.ENVValue)
		} else if injection.Value != "" {
			(*logEntry)[injection.Field] = injection.Value
		} else if injection.Hostname {
			hostname, _ := os.Hostname()
			(*logEntry)[injection.Field] = hostname
		}
	}
}

func dispatchEntry(event FlingEvent, outputs []string, channels map[string]interface{}) {
	for _, output := range outputs {
		channels[output].(chan FlingEvent) <- event
	}
}

func handleRotations(rotations []FlingRotation) {
	for _, rotation := range rotations {
		go rotateWorker(rotation)
	}
}

func rotateWorker(rotation FlingRotation) {
	for {
		log.WithFields(log.Fields{
			"seconds": rotation.RotateInterval,
		}).Info("Sleeping before rotate")

		time.Sleep(time.Duration(rotation.RotateInterval) * time.Second)

		rotate(rotation)
	}
}

func rotate(rotation FlingRotation) {
	//Handle the files first
	for _, path := range rotation.Files {
		files, _ := filepath.Glob(path)
		for _, file := range files {

			renameErr := os.Rename(file, file+".old")
			if renameErr != nil {
				log.WithFields(log.Fields{
					"path": file,
				}).Error("Unable to move log file in rotation")

				continue
			} else {
				log.WithFields(log.Fields{
					"path": file,
				}).Info("Moved log file")
			}
		}
	}

	// Perform rotation command
	if rotation.RotateCommand != "" {
		_, cmdError := exec.Command("sh", "-c", rotation.RotateCommand).Output()
		if cmdError != nil {
			log.WithFields(log.Fields{
				"error":   cmdError,
				"command": rotation.RotateCommand,
			}).Error("Rotate command failed")
		} else {
			log.WithFields(log.Fields{
				"command": rotation.RotateCommand,
			}).Info("Rotation command successful")
		}
	}
}

//Get an RFC 3339 Nano Time for use in log timestamps
func get3339Time() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}
