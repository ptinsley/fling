package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/hpcloud/tail"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/option"
	"github.com/alecthomas/kingpin/v2"
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
	AuthFile     string           `json:"auth_file,omitempty"`
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
	Path              string           `json:"path"`
	IsJSON            bool             `json:"is_json"`
	IsGlob            bool             `json:"is_glob"`
	GlobInterval      int              `json:"glob_interval"`
	ReadFromBeginning bool             `json:"read_from_beginning"`
	Outputs           []string         `json:"outputs"`
	Injections        []FlingInjection `json:"injections"`
	Multiline         *FlingMultiline  `json:"multiline,omitempty"`
}

//FlingMultiline - joins log records that span several physical lines into one
// event. Omit it to keep the default one-line-per-event behaviour.
type FlingMultiline struct {
	StartPattern     string `json:"start_pattern"`
	ConditionPattern string `json:"condition_pattern,omitempty"`
	Mode             string `json:"mode"`
	TimeoutMS        int    `json:"timeout_ms"`
	MaxLines         int    `json:"max_lines,omitempty"`
	MaxBytes         int    `json:"max_bytes,omitempty"`
}

// Multiline modes, matching Vector's file source so a pattern can be moved
// between the two without reinterpretation.
const (
	// multilineModeContinueThrough - consecutive lines matching ConditionPattern
	// belong to the record (indented stack traces).
	multilineModeContinueThrough = "continue_through"
	// multilineModeContinuePast - a line matching ConditionPattern means the
	// *next* line belongs to the record (trailing backslash continuations).
	multilineModeContinuePast = "continue_past"
	// multilineModeHaltBefore - the record ends before the next line matching
	// ConditionPattern (timestamp-prefixed logs).
	multilineModeHaltBefore = "halt_before"
	// multilineModeHaltWith - the record ends with the first line matching
	// ConditionPattern, inclusive.
	multilineModeHaltWith = "halt_with"
)

const (
	// multilineTruncatedFlag is added to ECS log.flags when a record is cut short
	// by MaxLines or MaxBytes, so a partial record is never mistaken for a whole one.
	multilineTruncatedFlag = "multiline_truncated"
	// multilineOversizedFlag is added instead when a single physical line met
	// MaxBytes on its own. That record is whole - nothing was cut from it - so
	// calling it truncated would be the inverse mistake, but it is still larger
	// than MaxBytes was set to allow.
	multilineOversizedFlag = "multiline_oversized_line"

	defaultMultilineMaxLines = 500
	defaultMultilineMaxBytes = 262144

	// Bounds on how often an assembling record is checked for going idle. The
	// floor bounds wake-up cost, so a timeout below twice the floor is checked
	// less often than half of itself and a record can wait timeout + floor.
	minFlushCheckInterval = 50 * time.Millisecond
	maxFlushCheckInterval = 250 * time.Millisecond

	// tailRecycleInterval - how often a tail is torn down and rebuilt to release
	// handles held on rotated files.
	tailRecycleInterval = time.Hour
	// tailRecycleQuietWindow - how quiet a file has to be before a due recycle
	// happens on an aggregating tail. The rebuilt tail seeks to the end of the
	// file, so a line written across the teardown is lost; waiting for a lull keeps
	// that window empty, and matches the silence the non-aggregating path relies on.
	tailRecycleQuietWindow = 5 * time.Second
	// tailRetryInterval - backoff before retrying a file that could not be tailed.
	tailRetryInterval = 10 * time.Second
)

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

	if validateErr := validateConfig(config); validateErr != nil {
		log.WithFields(log.Fields{
			"error": validateErr,
		}).Fatal("Invalid config")
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
	configString, readError := os.ReadFile(path)

	if readError != nil {
		return config, readError
	}

	parseError := json.Unmarshal(configString, &config)

	return config, parseError
}

//validateConfig - fail at startup rather than per-line on a bad multiline block
func validateConfig(config FlingConfig) error {
	files := append(append([]FlingInFile{}, config.Input.Files...), config.Files...)

	for _, file := range files {
		if file.Multiline == nil {
			continue
		}

		if _, err := newMultilineAggregator(*file.Multiline); err != nil {
			return fmt.Errorf("input file %q: multiline: %w", file.Path, err)
		}
	}

	return nil
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
	ctx := context.Background()

	if project == "" {
		log.Fatal("PubSub output project must be defined")
		panic("Invalid Config")
	}
	if topicName == "" {
		log.Fatal("PubSub output topic must be defined")
		panic("Invalid Config")
	}

	var pubSubClient *pubsub.Client
	var err error

	if authfile == "" {
		pubSubClient, err = pubsub.NewClient(ctx, project)
	} else {
		pubSubClient, err = pubsub.NewClient(ctx, project, option.WithCredentialsFile(authfile))
	}

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
		tailConfig := tail.Config{Follow: true, ReOpen: true, Poll: !*inotifyFlag}
		if !file.ReadFromBeginning {
			tailConfig.Location = &tail.SeekInfo{Offset: 0, Whence: io.SeekEnd}
		}

		t, tailErr := tail.TailFile(file.Path, tailConfig)

		if tailErr != nil {
			log.WithFields(log.Fields{
				"path":  file.Path,
				"error": tailErr,
			}).Error("Couldn't tail file")

			// There is no tail to read from, so back off rather than falling
			// through and dereferencing it.
			time.Sleep(tailRetryInterval)

			continue
		}

		log.Info("tailed log")

		var aggregator *multilineAggregator
		if file.Multiline != nil {
			var aggErr error
			aggregator, aggErr = newMultilineAggregator(*file.Multiline)
			if aggErr != nil {
				//Config is validated at startup, so reaching this means a bug
				log.WithFields(log.Fields{
					"path":  file.Path,
					"error": aggErr,
				}).Fatal("Couldn't build multiline aggregator")
			}
		}

		// Only armed when aggregating; a nil channel blocks forever in a select, so
		// the non-multiline path never wakes up for this. While aggregating it also
		// carries the recycle deadline, which cannot be a timer in the select - see
		// the recycle comment below.
		var flushTicker *time.Ticker
		var flushTick <-chan time.Time
		if aggregator != nil {
			flushTicker = time.NewTicker(aggregator.flushInterval())
			flushTick = flushTicker.C
		}

		recycleAt := time.Now().Add(tailRecycleInterval)
		lastLine := time.Now()

		reopen := false

	Processing:
		for {
			select {
			case line := <-t.Lines:
				if line == nil {
					// Lines was closed, so the tailer goroutine has already exited
					// and there is nothing left to drain. This happens on a watcher
					// error or a permanently failed reopen, which can land
					// mid-record - flush or the buffered head of the record dies
					// with the tail.
					flushPartialAndDispatch(aggregator, file, outputs)

					// Back off before re-tailing. This is the reachable failure
					// path - TailFile only returns an error when MustExist is set,
					// which fling never does - so without a delay a permanently
					// missing file spins hot.
					reopen = true

					break Processing
				}

				lastLine = line.Time
				dispatchLine(line, aggregator, file, outputs)
			case tick := <-flushTick:
				if event, ok := aggregator.idleFlush(tick); ok {
					dispatchEvent(event, file, outputs)
				}

				// The recycle deadline has to be absolute on this path. Deriving it
				// from a timer in the select re-arms it on every wake-up, and the
				// flush ticker above wakes this loop several times a second, so an
				// aggregating tail would never be recycled at all. Wait for the file
				// to fall quiet first: it stands in for the silence the timer used to
				// guarantee, keeping the teardown out of the way of a line in flight
				// and out of the way of the seek-to-end the rebuilt tail performs.
				//
				// Read the clock rather than trusting the tick. A Ticker drops ticks
				// when this loop stalls - dispatch blocks whenever an output channel
				// fills - so the tick time can be arbitrarily stale, and a stale one
				// here defers the recycle for as long as the stall lasts. A stale tick
				// only delays an idle flush by one interval, so idleFlush above can
				// keep using it.
				now := time.Now()
				if now.Before(recycleAt) || now.Sub(lastLine) < tailRecycleQuietWindow {
					continue
				}

				// Kill and drain rather than Stop(). tail.Lines is unbuffered and
				// sendLine has no Dying() case, so a line in flight blocks the tailer
				// goroutine until something receives it - and Stop() waits for that
				// goroutine, so it would wedge both permanently. Draining until Lines
				// closes unblocks the tailer and keeps the lines it had already read.
				t.Kill(nil)
				for line := range t.Lines {
					dispatchLine(line, aggregator, file, outputs)
				}

				// Anything still assembling was cut short by the teardown rather than
				// by its own delimiter, so it must not look like a complete record.
				flushPartialAndDispatch(aggregator, file, outputs)

				break Processing
			case <-legacyRecycleTick(aggregator):
				// Non-aggregating tails keep the original deadline exactly: it is
				// re-armed on every line, so it only fires after an hour of silence
				// rather than hourly, and that silence is what makes this t.Stop()
				// safe. tail.Lines is unbuffered and sendLine blocks on it with no
				// Dying() case, while Stop() waits for the tailer goroutine, so
				// stopping with a line in flight would wedge both permanently.
				t.Stop()

				break Processing
			}
		}

		if flushTicker != nil {
			flushTicker.Stop()
		}

		if reopen {
			time.Sleep(tailRetryInterval)
		}
	}
}

//legacyRecycleTick - the recycle deadline for a tail with no aggregator, armed
// fresh on every pass of the select as it always was. An aggregating tail gets nil,
// which parks the case forever: it carries its own absolute deadline instead.
func legacyRecycleTick(aggregator *multilineAggregator) <-chan time.Time {
	if aggregator != nil {
		return nil
	}

	return time.After(tailRecycleInterval)
}

//dispatchLine - feed one physical line through the aggregator, if there is one,
// and dispatch whatever records completed
func dispatchLine(line *tail.Line, aggregator *multilineAggregator, file FlingInFile, outputs map[string]interface{}) {
	if aggregator == nil {
		// A zero Lines omits the multiline field entirely, so events from
		// non-multiline sources are byte-identical to before.
		dispatchEvent(aggregatedEvent{Text: line.Text, Time: line.Time}, file, outputs)

		return
	}

	for _, event := range aggregator.add(line.Text, line.Time) {
		dispatchEvent(event, file, outputs)
	}
}

//flushPartialAndDispatch - emit a record cut short by a tail teardown, if one was
// still assembling
func flushPartialAndDispatch(aggregator *multilineAggregator, file FlingInFile, outputs map[string]interface{}) {
	if aggregator == nil {
		return
	}

	if event, ok := aggregator.flushPartial(); ok {
		dispatchEvent(event, file, outputs)
	}
}

//aggregatedEvent - one assembled log record, ready to become a FlingEvent
type aggregatedEvent struct {
	Text      string
	Time      time.Time
	Truncated bool
	// Oversized marks a single physical line that met MaxBytes by itself. Nothing
	// was cut from it, so a limit firing on it does not set Truncated - but a chunk
	// can be both when a lone oversized line is also the tail of a record an
	// earlier limit cut.
	Oversized bool
	// Lines is how many physical lines were joined. Emitted as
	// fling.multiline.line_count so a mis-tuned pattern is visible downstream:
	// all-1 means the pattern never matched, values pinned at MaxLines mean it is
	// merging unrelated records.
	Lines int
}

//multilineAggregator - buffers physical lines until a complete record is seen.
// It performs no I/O so it can be exercised directly in tests.
type multilineAggregator struct {
	start    *regexp.Regexp
	cond     *regexp.Regexp
	mode     string
	timeout  time.Duration
	maxLines int
	maxBytes int

	buf             []string
	bytes           int
	firstTime       time.Time
	lastAppend      time.Time
	lastMatchedCond bool
	// truncated is set when a limit cut *this* chunk short, as opposed to draining
	// below, which says the chunk is the tail of one cut earlier. The two are
	// separate because a chunk can be either without being the other.
	truncated bool
	// oversized is set when the limit that fired was MaxBytes against a single
	// line, which cuts nothing. Independent of truncated and draining: a lone
	// oversized line can equally be the tail of a record cut earlier.
	oversized bool
	// draining is set when a limit cut a record short. The lines that follow are
	// the tail of that record rather than records of their own, so they keep being
	// assembled - into further chunks, each still flagged truncated - until a start
	// line opens a genuine new record or the file goes quiet. Without it the
	// remainder of a runaway record shipped as unflagged one-line events, which is
	// the shredding this whole feature exists to prevent.
	draining bool
}

func newMultilineAggregator(config FlingMultiline) (*multilineAggregator, error) {
	switch config.Mode {
	case multilineModeContinueThrough, multilineModeContinuePast,
		multilineModeHaltBefore, multilineModeHaltWith:
	case "":
		return nil, fmt.Errorf("mode is required (one of %s, %s, %s, %s)",
			multilineModeContinueThrough, multilineModeContinuePast,
			multilineModeHaltBefore, multilineModeHaltWith)
	default:
		return nil, fmt.Errorf("unknown mode %q", config.Mode)
	}

	if config.StartPattern == "" {
		return nil, fmt.Errorf("start_pattern is required")
	}

	start, startErr := regexp.Compile(config.StartPattern)
	if startErr != nil {
		return nil, fmt.Errorf("start_pattern %q: %w", config.StartPattern, startErr)
	}

	conditionPattern := config.ConditionPattern
	if conditionPattern == "" {
		// halt_before is the one mode with a sane default: a record runs until the
		// next line that looks like a start line. Everything else would be a silent
		// no-op if defaulted - the continue modes match continuation lines, which
		// never look like a start line, and halt_with would close each record on
		// the very line that opened it, making every record exactly one line.
		if config.Mode != multilineModeHaltBefore {
			return nil, fmt.Errorf("condition_pattern is required for mode %s", config.Mode)
		}
		conditionPattern = config.StartPattern
	}

	cond, condErr := regexp.Compile(conditionPattern)
	if condErr != nil {
		return nil, fmt.Errorf("condition_pattern %q: %w", conditionPattern, condErr)
	}

	if config.TimeoutMS <= 0 {
		return nil, fmt.Errorf("timeout_ms must be greater than 0")
	}

	maxLines := config.MaxLines
	if maxLines <= 0 {
		maxLines = defaultMultilineMaxLines
	}

	maxBytes := config.MaxBytes
	if maxBytes <= 0 {
		maxBytes = defaultMultilineMaxBytes
	}

	return &multilineAggregator{
		start:    start,
		cond:     cond,
		mode:     config.Mode,
		timeout:  time.Duration(config.TimeoutMS) * time.Millisecond,
		maxLines: maxLines,
		maxBytes: maxBytes,
	}, nil
}

// add feeds one physical line in and returns whatever records that completed. A
// single line can close the buffered record and open the next one, so more than
// one event can come back at once.
func (a *multilineAggregator) add(text string, lineTime time.Time) []aggregatedEvent {
	events := make([]aggregatedEvent, 0, 2)

	// halt_before ends the record *before* the matching line is consumed. That
	// line then opens the next record unconditionally, as it does in Vector -
	// re-checking start_pattern here would orphan it whenever condition_pattern
	// is deliberately broader than start_pattern. It opens one whether or not a
	// record is buffered: with an empty buffer it is still this mode's record
	// boundary, and the buffer is routinely empty here because a limit just cut a
	// record. Requiring one left the drain that limit started running straight
	// across the boundary, flagging the clean record that follows.
	opensNext := false
	if a.mode == multilineModeHaltBefore && a.cond.MatchString(text) {
		if len(a.buf) > 0 {
			events = append(events, a.take())
		}

		opensNext = true
	}

	if len(a.buf) > 0 && !a.continues(text) {
		events = append(events, a.take())
	}

	startsRecord := a.start.MatchString(text)

	if len(a.buf) == 0 && !opensNext && !startsRecord && !a.draining {
		// Either fling attached part-way through a record, or the pattern does not
		// fit this content. Emit the line on its own - never drop it.
		return append(events, aggregatedEvent{Text: text, Time: lineTime, Lines: 1})
	}

	if len(a.buf) == 0 && (startsRecord || opensNext) {
		// A new record opens here, so whatever a limit cut short earlier is over.
		a.draining = false
	}

	a.appendLine(text, lineTime)

	// Evaluated first and short circuited, so a record closed by its own delimiter
	// never consults the limits and never starts a drain.
	closed := a.closesRecord(text)
	limitFired := !closed && a.overLimit()

	if closed || limitFired {
		events = append(events, a.take())

		if limitFired {
			// The rest of this record is still coming; keep assembling it rather than
			// letting it fall out as unflagged single lines. Set after take() so the
			// chunk the limit just cut is flagged by a.truncated alone - an oversized
			// whole line sets neither flag and must not pick one up from here.
			a.draining = true
		}

		// The record's own delimiter arrived, so a record a limit cut short has ended
		// here and the lines that follow are records of their own, not more of its
		// tail. Cleared after take() so this final chunk still carries the flag.
		if closed {
			a.draining = false
		}
	}

	return events
}

// continues reports whether text belongs to the record already buffered.
func (a *multilineAggregator) continues(text string) bool {
	switch a.mode {
	case multilineModeContinueThrough:
		return a.cond.MatchString(text)
	case multilineModeContinuePast:
		// Always true in practice: a continue_past record is closed by
		// closesRecord on the first line lacking the marker, so a buffer only
		// survives to the next call with lastMatchedCond set. Kept explicit rather
		// than folded into the default so the mode's rule stays readable.
		return a.lastMatchedCond
	default:
		// halt_before has already been handled by the caller; halt_with keeps
		// consuming until closesRecord fires.
		return true
	}
}

// closesRecord reports whether text is the final line of the record.
func (a *multilineAggregator) closesRecord(text string) bool {
	switch a.mode {
	case multilineModeHaltWith:
		return a.cond.MatchString(text)
	case multilineModeContinuePast:
		return !a.cond.MatchString(text)
	default:
		// continue_through and halt_before are closed by a later line or by timeout.
		return false
	}
}

func (a *multilineAggregator) appendLine(text string, lineTime time.Time) {
	if len(a.buf) == 0 {
		a.firstTime = lineTime
	} else {
		a.bytes++ // the newline this line is joined with
	}

	a.buf = append(a.buf, text)
	a.bytes += len(text)
	a.lastAppend = lineTime
	a.lastMatchedCond = a.cond.MatchString(text)
}

// overLimit reports whether the buffered record has hit MaxLines or MaxBytes, and
// records which kind of limit fired. The caller starts the drain; doing it here
// would flag the chunk being taken as the tail of itself.
func (a *multilineAggregator) overLimit() bool {
	if len(a.buf) < a.maxLines && a.bytes < a.maxBytes {
		return false
	}

	// A lone line that met MaxBytes by itself was not cut short - it arrived whole
	// and is merely bigger than the ceiling. Calling that truncated is the inverse
	// of the mistake the flag exists to prevent, so it gets its own flag. A
	// MaxLines of 1 genuinely does cut every record, so it stays truncated.
	if len(a.buf) == 1 && a.bytes >= a.maxBytes && a.maxLines > 1 {
		a.oversized = true
	} else {
		a.truncated = true
	}

	return true
}

// take removes the buffered record and returns it.
func (a *multilineAggregator) take() aggregatedEvent {
	event := aggregatedEvent{
		Text: strings.Join(a.buf, "\n"),
		Time: a.firstTime,
		// A drain chunk is the tail of a record a limit already cut, so it belongs
		// to a truncated record even when nothing cut this chunk itself.
		Truncated: a.truncated || a.draining,
		Oversized: a.oversized,
		Lines:     len(a.buf),
	}

	a.buf = nil
	a.bytes = 0
	a.truncated = false
	a.oversized = false
	a.lastMatchedCond = false

	return event
}

// flush emits the buffered record as-is, if there is one.
func (a *multilineAggregator) flush() (aggregatedEvent, bool) {
	if len(a.buf) == 0 {
		return aggregatedEvent{}, false
	}

	return a.take(), true
}

// idleFlush emits the buffered record once the file has been quiet for longer than
// the timeout. Whether going quiet means the record ended legitimately depends on
// the mode: a halt_before or continue_through record is closed by the absence of
// further lines, but a halt_with record never saw its terminator and a
// continue_past record never saw the line its last marker promised, so those two
// were cut short and are flagged.
func (a *multilineAggregator) idleFlush(now time.Time) (aggregatedEvent, bool) {
	if now.Sub(a.lastAppend) < a.timeout {
		return aggregatedEvent{}, false
	}

	incomplete := a.mode == multilineModeHaltWith ||
		(a.mode == multilineModeContinuePast && a.lastMatchedCond)

	event, ok := a.flush()
	if ok && incomplete {
		event.Truncated = true
	}

	// The file has gone quiet, so a record a limit cut short has ended too - the
	// next lines to arrive are new records, not its tail.
	a.draining = false

	return event, ok
}

// flushPartial emits the buffered record because something other than its own
// delimiter cut it short - currently a tail teardown. Marked truncated so a
// severed record is never mistaken for a complete one.
func (a *multilineAggregator) flushPartial() (aggregatedEvent, bool) {
	event, ok := a.flush()
	if ok {
		event.Truncated = true
	}

	return event, ok
}

// flushInterval is how often to check for an idle record. Checking only at the
// timeout itself would let a record wait up to twice the timeout to be emitted.
func (a *multilineAggregator) flushInterval() time.Duration {
	interval := a.timeout / 2

	if interval > maxFlushCheckInterval {
		interval = maxFlushCheckInterval
	}
	if interval < minFlushCheckInterval {
		interval = minFlushCheckInterval
	}

	return interval
}

//setLogFlag - add a flag to ECS log.flags following whichever shape the source
// already uses. An is_json source emitting nested ECS carries a "log" object, and
// writing the flat dotted key alongside it would put the same field in the
// document twice; fling's own plain-text events have no "log" object, so they get
// the dotted key the rest of its fields use.
func setLogFlag(logEntry map[string]interface{}, flag string) {
	if nested, ok := logEntry["log"].(map[string]interface{}); ok {
		nested["flags"] = appendLogFlag(nested["flags"], flag)

		return
	}

	logEntry["log.flags"] = appendLogFlag(logEntry["log.flags"], flag)
}

//appendLogFlag - add a flag to a log.flags value without discarding any the
// application already emitted on an is_json source
func appendLogFlag(existing interface{}, flag string) []string {
	var flags []string

	switch typed := existing.(type) {
	case []string:
		flags = append(flags, typed...)
	case []interface{}:
		for _, value := range typed {
			if text, ok := value.(string); ok {
				flags = append(flags, text)
			}
		}
	case string:
		flags = append(flags, typed)
	}

	return append(flags, flag)
}

//dispatchEvent - turn one assembled record into a FlingEvent and hand it to the
// outputs the input is wired to
func dispatchEvent(event aggregatedEvent, file FlingInFile, outputs map[string]interface{}) {
	var logEntry map[string]interface{}

	log.WithFields(log.Fields{
		"path": file.Path,
		"line": event.Text,
	}).Debug("Processing log line")

	if file.IsJSON {
		unmarshalErr := json.Unmarshal([]byte(event.Text), &logEntry)
		if unmarshalErr != nil {
			log.WithFields(log.Fields{
				"message": event.Text,
				"error":   unmarshalErr,
			}).Error("Couldn't parse JSON log line")

			return
		}
	} else {
		logEntry = make(map[string]interface{})
		logEntry["message"] = event.Text
	}

	//FIXME: Inject other pertinent context info
	logEntry["fling.source"] = file.Path

	if _, ok := logEntry["@timestamp"]; !ok {
		logEntry["@timestamp"] = timestampFor(event.Time)
	}

	// Not mutually exclusive: a lone oversized line can also be the tail of a record
	// an earlier limit cut, and then both facts hold of it.
	if event.Truncated {
		setLogFlag(logEntry, multilineTruncatedFlag)
	}
	if event.Oversized {
		setLogFlag(logEntry, multilineOversizedFlag)
	}

	// Only set when aggregating. Makes a mis-tuned pattern diagnosable from the
	// index itself: every record at 1 means the pattern never matched, records
	// pinned at max_lines mean it is swallowing unrelated lines.
	if event.Lines > 0 {
		logEntry["fling.multiline.line_count"] = event.Lines
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
		// add a random offset within a 10 minute window to try and prevent
		//  thundering herd
		window, err := strconv.Atoi(os.Getenv("ROTATE_WINDOW"))

		if err != nil || window < 1 {
			//use a default of 10 minutes
			window = 600
		}

		interval := rotation.RotateInterval + rand.Intn(window)

		log.WithFields(log.Fields{
			"seconds": interval,
		}).Info("Sleeping before rotate")

		time.Sleep(time.Duration(interval) * time.Second)

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

//timestampFor - stamp an event with when its first line was read rather than when
// it was dispatched; assembling a multi-line record can take until its flush timeout.
func timestampFor(lineTime time.Time) string {
	if lineTime.IsZero() {
		return get3339Time()
	}

	return lineTime.UTC().Format(time.RFC3339Nano)
}
