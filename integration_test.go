package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
)

// End-to-end coverage of the real publish path: a compiled fling binary tails
// files and ships to a Pub/Sub emulator, and we assert on what actually landed
// on the subscription.
//
// This does not skip. Pub/Sub is the point of the tool, so a missing emulator is
// a broken environment, not a reason to report success — every prerequisite
// check below calls t.Fatal with the command that fixes it.
//
// Every wait here is bounded and every bound is an assertion: on expiry the
// failure names what it was waiting for and what it had instead. Nothing in this
// file sleeps for a fixed duration hoping the system caught up.

const (
	integrationProject = "test-project"
	integrationTopic   = "fling-test"
	integrationSub     = "fling-test-sub"

	// Bounds. Each one fails the test with a diagnostic rather than hanging
	// until go test's global -timeout fires.
	emulatorReadyTimeout = 30 * time.Second
	tailReadyTimeout     = 30 * time.Second
	globDiscoverTimeout  = 20 * time.Second
	messageWaitTimeout   = 60 * time.Second

	// After every expected count is met, keep draining briefly so that a source
	// publishing MORE than expected still trips the exact-count assertion.
	settleDelay = 2 * time.Second

	// Covers the gap between fling logging that it is tailing a file and the
	// worker goroutine actually seeking to end. See the comment at its use.
	tailSettleDelay = 2 * time.Second

	// Matches the glob_interval in the generated config.
	globInterval = 2
)

type expectation struct {
	Generator     string
	Description   string
	ExpectedCount int
	IsJSON        bool
	// Lines that MUST be present (substring match on "message" field)
	RequiredLines []string
	// Lines that MUST NOT be present
	ForbiddenLines []string
	// Each group holds substrings that MUST all appear in the SAME message.
	// This is what proves multi-line assembly - RequiredLines only proves a
	// substring arrived somewhere, which a shredded record also satisfies.
	RequiredJoined [][]string
	// A value that MUST appear in some message's ECS "log.flags" array
	RequiredFlag string
	// A value that MUST appear in EVERY message's ECS "log.flags" array. Once a
	// limit cuts a record short, the lines after the cut are still that record's
	// tail, so nothing from a truncation scenario may arrive looking whole.
	RequiredFlagOnAll string
	// When set, every message MUST carry "fling.multiline.line_count" and it MUST
	// equal the actual number of newline-separated lines in "message"
	CheckLineCount bool
}

func integrationExpectations() []expectation {
	return []expectation{
		{
			Generator:      "scenario-1-static",
			Description:    "Static file, default read (tail)",
			ExpectedCount:  2,
			RequiredLines:  []string{"static-appended-1", "static-appended-2"},
			ForbiddenLines: []string{"static-prepop-1", "static-prepop-2", "static-prepop-3"},
		},
		{
			Generator:     "scenario-2-static-beginning",
			Description:   "Static file, read_from_beginning",
			ExpectedCount: 5,
			RequiredLines: []string{"beginning-prepop-1", "beginning-prepop-2", "beginning-prepop-3", "beginning-appended-1", "beginning-appended-2"},
		},
		{
			Generator:     "scenario-3-glob",
			Description:   "Glob pattern, default read (tail)",
			ExpectedCount: 2,
			RequiredLines: []string{"glob-appended-1", "glob-appended-2"},
		},
		{
			Generator:     "scenario-4-glob-beginning",
			Description:   "Glob pattern, read_from_beginning",
			ExpectedCount: 3,
			RequiredLines: []string{"glob-beginning-prepop-1", "glob-beginning-prepop-2", "glob-beginning-appended-1"},
		},
		{
			Generator:     "scenario-5-json",
			Description:   "JSON file, read_from_beginning",
			ExpectedCount: 3,
			IsJSON:        true,
			RequiredLines: []string{"json-prepop-1", "json-prepop-2", "json-appended-1"},
		},
		{
			Generator:      "scenario-6-multiline-mysql-error",
			Description:    "Multiline halt_before, MySQL crash backtrace",
			ExpectedCount:  2,
			RequiredLines:  []string{"Shutdown complete"},
			CheckLineCount: true,
			RequiredJoined: [][]string{
				// The whole 5-line crash record must arrive as ONE document.
				{"Assertion failure", "memory trap", "Thread pointer", "my_print_stacktrace"},
			},
		},
		{
			Generator:      "scenario-10-multiline-mysql-slow",
			Description:    "Multiline halt_before, MySQL slow query log incl. omitted # Time:",
			ExpectedCount:  4,
			CheckLineCount: true,
			RequiredJoined: [][]string{
				// Query spanning two SQL lines stays whole.
				{"Id: 12345", "Query_time: 2.345678", "AND created_at"},
				// The entry that omitted "# Time:" must not be swallowed by its
				// neighbour - this is the case a "# Time:"-anchored pattern breaks.
				{"Id: 12346", "Query_time: 1.111", "SELECT COUNT(*)"},
			},
		},
		{
			Generator:      "scenario-7-multiline-nginx-error",
			Description:    "Multiline halt_before, nginx error with app stderr passthrough",
			ExpectedCount:  2,
			RequiredLines:  []string{"upstream server temporarily disabled"},
			CheckLineCount: true,
			RequiredJoined: [][]string{
				{"Passenger: app error", "StatementInvalid", "order.rb:88"},
			},
		},
		{
			Generator:     "scenario-8-multiline-timeout",
			Description:   "Multiline idle timeout flushes an unterminated record",
			ExpectedCount: 1,
			RequiredLines: []string{"lone record awaiting timeout flush"},
		},
		{
			Generator:      "scenario-9-multiline-truncation",
			Description:    "Multiline max_lines truncation is flagged, remainder kept and flagged",
			ExpectedCount:  2,
			CheckLineCount: true,
			RequiredJoined: [][]string{
				{"runaway record begins", "runaway line 1", "runaway line 2"},
			},
			// Cut at 3 lines, so line 3 falls outside the capped record and arrives in
			// a follow-on chunk. Both halves carry the flag: the tail of a record a
			// limit cut short must not look like a record of its own.
			RequiredLines:     []string{"runaway line 3"},
			RequiredFlagOnAll: "multiline_truncated",
		},
	}
}

// hasLogFlag reports whether a message carries flag in ECS log.flags, in either
// shape fling writes: the flat dotted key on a plain-text event, or inside the
// "log" object an is_json source already emitted.
func hasLogFlag(msg map[string]interface{}, flag string) bool {
	raw := msg["log.flags"]
	if nested, ok := msg["log"].(map[string]interface{}); ok {
		raw = nested["flags"]
	}

	flags, ok := raw.([]interface{})
	if !ok {
		return false
	}

	for _, value := range flags {
		if text, ok := value.(string); ok && text == flag {
			return true
		}
	}

	return false
}

// safeBuffer collects a subprocess's output while the test polls it for
// readiness markers.
type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *safeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// collector accumulates everything published to the subscription, keyed by the
// injected "generator" field.
type collector struct {
	mu       sync.Mutex
	received map[string][]map[string]interface{}
	total    int
}

func (c *collector) add(gen string, data map[string]interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.received[gen] = append(c.received[gen], data)
	c.total++
}

func (c *collector) forGenerator(gen string) []map[string]interface{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]map[string]interface{}(nil), c.received[gen]...)
}

func (c *collector) counts() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]int, len(c.received))
	for gen, msgs := range c.received {
		out[gen] = len(msgs)
	}
	return out
}

// waitFor polls cond until it returns true or the bound expires. It reports
// whether cond ever held, leaving the diagnostic to the caller, which knows what
// it was waiting for.
func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// requirePrereqs fails with the exact remediation command for whatever is
// missing. The emulator is not optional.
func requirePrereqs(t *testing.T) string {
	t.Helper()

	if _, err := exec.LookPath("gcloud"); err != nil {
		t.Fatal("gcloud CLI not found. Install the Google Cloud SDK: https://cloud.google.com/sdk/docs/install")
	}

	state, err := exec.Command("gcloud", "components", "list",
		"--filter=id:pubsub-emulator", "--format=value(state.name)").Output()
	if err != nil || strings.TrimSpace(string(state)) == "" || strings.Contains(string(state), "Not Installed") {
		t.Fatal("Pub/Sub emulator component not installed. Run: gcloud components install pubsub-emulator")
	}

	if _, err := exec.LookPath("java"); err != nil {
		t.Fatal("java not found on PATH. The Pub/Sub emulator requires a JRE.\n" +
			"  macOS: brew install openjdk@21 && export PATH=\"/opt/homebrew/opt/openjdk@21/bin:$PATH\"")
	}
	// The /usr/bin/java shim on macOS resolves even with no JDK installed, so
	// confirm a runtime actually answers before blaming the emulator later.
	if out, err := exec.Command("java", "-version").CombinedOutput(); err != nil {
		t.Fatalf("java is on PATH but not runnable: %v\n%s\n"+
			"  macOS: brew install openjdk@21 && export PATH=\"/opt/homebrew/opt/openjdk@21/bin:$PATH\"", err, out)
	}

	// Prefer the emulator's own launcher over "gcloud beta emulators pubsub",
	// which needs the beta component. Homebrew installs of the SDK often ship
	// the emulator without beta, and the launcher takes the same host/port.
	root, err := exec.Command("gcloud", "info", "--format=value(installation.sdk_root)").Output()
	if err == nil {
		launcher := filepath.Join(strings.TrimSpace(string(root)),
			"platform", "pubsub-emulator", "bin", "cloud-pubsub-emulator")
		if info, statErr := os.Stat(launcher); statErr == nil && info.Mode()&0111 != 0 {
			return launcher
		}
	}

	beta, err := exec.Command("gcloud", "components", "list",
		"--filter=id:beta", "--format=value(state.name)").Output()
	if err != nil || strings.TrimSpace(string(beta)) == "" || strings.Contains(string(beta), "Not Installed") {
		t.Fatal("need either the emulator launcher or the gcloud beta component. Run: gcloud components install beta")
	}
	return ""
}

func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("could not reserve a port for the emulator: %v", err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

// startEmulator boots the emulator and blocks until it answers, or fails the
// test with the emulator's own output.
func startEmulator(t *testing.T, launcher string, port int) string {
	t.Helper()

	host := fmt.Sprintf("localhost:%d", port)
	var cmd *exec.Cmd
	if launcher != "" {
		cmd = exec.Command(launcher, "--host=localhost", fmt.Sprintf("--port=%d", port))
	} else {
		cmd = exec.Command("gcloud", "beta", "emulators", "pubsub", "start",
			"--project="+integrationProject, "--host-port="+host)
	}

	out := &safeBuffer{}
	cmd.Stdout = out
	cmd.Stderr = out
	if err := cmd.Start(); err != nil {
		t.Fatalf("could not start the Pub/Sub emulator: %v", err)
	}

	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})

	client := &http.Client{Timeout: time.Second}
	ready := waitFor(emulatorReadyTimeout, func() bool {
		if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
			return false
		}
		resp, err := client.Get("http://" + host)
		if err != nil {
			return false
		}
		resp.Body.Close()
		return true
	})
	if !ready {
		t.Fatalf("emulator did not answer on %s within %s\nemulator output:\n%s",
			host, emulatorReadyTimeout, out.String())
	}

	return host
}

func writeFixtures(t *testing.T, dir string) {
	t.Helper()

	files := map[string]string{
		// Scenario 1: pre-populated, read_from_beginning=false.
		"static.log": "static-prepop-1\nstatic-prepop-2\nstatic-prepop-3\n",
		// Scenario 2: pre-populated, read_from_beginning=true.
		"static_beginning.log": "beginning-prepop-1\nbeginning-prepop-2\nbeginning-prepop-3\n",
		// Scenario 4: pre-populated glob dir, read_from_beginning=true.
		"glob_beginning/app.log": "glob-beginning-prepop-1\nglob-beginning-prepop-2\n",
		// Scenario 5: pre-populated JSON, read_from_beginning=true.
		"json.log": `{"message":"json-prepop-1","level":"info","service":"test"}` + "\n" +
			`{"message":"json-prepop-2","level":"warn","service":"test"}` + "\n",
		// Scenario 6: a crash backtrace (one record over 5 lines) followed by a
		// separate timestamped line. Expect exactly 2 events.
		"mysql_error.log": `2026-07-28T10:23:45.123456Z 0 [ERROR] [MY-013183] [InnoDB] Assertion failure: trx0trx.cc:1234
InnoDB: We intentionally generate a memory trap.
Thread pointer: 0x7f8b40000f00
Attempting backtrace. You can use the following information to find out
/usr/sbin/mysqld(my_print_stacktrace+0x41) [0x21f0b41]
2026-07-28T10:23:46.000000Z 0 [System] [MY-010910] [Server] Shutdown complete
`,
		// Scenario 10: three queries. The second omits "# Time:" because it
		// landed in the same second as the first, which is why the pattern
		// anchors on "# User@Host:". Expect 4 events: the leading "# Time:"
		// alone, then one per query.
		"mysql_slow.log": `# Time: 2026-07-28T10:23:45.123456Z
# User@Host: app[app] @ 10.0.0.5 [10.0.0.5]  Id: 12345
# Query_time: 2.345678  Lock_time: 0.000123 Rows_sent: 1  Rows_examined: 1000000
SET timestamp=1785316225;
SELECT * FROM orders WHERE customer_id = 42
  AND created_at > '2026-01-01';
# User@Host: app[app] @ 10.0.0.6 [10.0.0.6]  Id: 12346
# Query_time: 1.111  Lock_time: 0.000 Rows_sent: 5  Rows_examined: 900
SELECT COUNT(*) FROM line_items;
# Time: 2026-07-28T10:23:47.000000Z
# User@Host: app[app] @ 10.0.0.7 [10.0.0.7]  Id: 12347
# Query_time: 3.5  Lock_time: 0.0 Rows_sent: 1  Rows_examined: 10
SELECT SLEEP(3);
`,
		// Scenario 7: an entry with app stderr passthrough beneath it, then a
		// second entry. Expect exactly 2 events.
		"nginx_error.log": `2026/07/28 10:23:45 [error] 1234#0: *567 Passenger: app error, client: 10.0.0.1
App 4242 stderr: ActiveRecord::StatementInvalid (Mysql2::Error: Lost connection)
App 4242 stderr:   from /webapp/app/models/order.rb:88
2026/07/28 10:23:47 [warn] 1234#0: *568 upstream server temporarily disabled
`,
		// Scenario 8: a lone record is appended later with nothing following it,
		// so only the idle timeout can flush it.
		"timeout.log": "",
		// Scenario 9: max_lines is 3, so the record is cut short and the leftover
		// line arrives in a follow-on chunk that is flagged too. Expect 2 events,
		// both marked multiline_truncated.
		"truncate.log": `2026-07-28T10:23:45.000000Z 0 [ERROR] runaway record begins
runaway line 1
runaway line 2
runaway line 3
`,
	}

	// Scenario 3's glob dir starts empty; the file is created after fling is up.
	if err := os.MkdirAll(filepath.Join(dir, "glob"), 0755); err != nil {
		t.Fatalf("could not create glob dir: %v", err)
	}

	for name, body := range files {
		path := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatalf("could not create dir for %s: %v", name, err)
		}
		if err := os.WriteFile(path, []byte(body), 0644); err != nil {
			t.Fatalf("could not write fixture %s: %v", name, err)
		}
	}
}

func writeConfig(t *testing.T, dir string) string {
	t.Helper()

	const tmpl = `{
  "input": {
    "files": [
      {
        "path": "%[1]s/static.log",
        "is_json": false, "is_glob": false, "read_from_beginning": false,
        "outputs": ["test-output"],
        "injections": [{"field": "generator", "value": "scenario-1-static"}]
      },
      {
        "path": "%[1]s/static_beginning.log",
        "is_json": false, "is_glob": false, "read_from_beginning": true,
        "outputs": ["test-output"],
        "injections": [{"field": "generator", "value": "scenario-2-static-beginning"}]
      },
      {
        "path": "%[1]s/glob/*.log",
        "is_json": false, "is_glob": true, "glob_interval": %[4]d, "read_from_beginning": false,
        "outputs": ["test-output"],
        "injections": [{"field": "generator", "value": "scenario-3-glob"}]
      },
      {
        "path": "%[1]s/glob_beginning/*.log",
        "is_json": false, "is_glob": true, "glob_interval": %[4]d, "read_from_beginning": true,
        "outputs": ["test-output"],
        "injections": [{"field": "generator", "value": "scenario-4-glob-beginning"}]
      },
      {
        "path": "%[1]s/json.log",
        "is_json": true, "is_glob": false, "read_from_beginning": true,
        "outputs": ["test-output"],
        "injections": [{"field": "generator", "value": "scenario-5-json"}]
      },
      {
        "path": "%[1]s/mysql_error.log",
        "is_json": false, "is_glob": false, "read_from_beginning": true,
        "outputs": ["test-output"],
        "multiline": {
          "start_pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}:[0-9]{2}",
          "mode": "halt_before", "timeout_ms": 500
        },
        "injections": [{"field": "generator", "value": "scenario-6-multiline-mysql-error"}]
      },
      {
        "path": "%[1]s/mysql_slow.log",
        "is_json": false, "is_glob": false, "read_from_beginning": true,
        "outputs": ["test-output"],
        "multiline": {
          "start_pattern": "^# User@Host:",
          "mode": "halt_before", "timeout_ms": 500
        },
        "injections": [{"field": "generator", "value": "scenario-10-multiline-mysql-slow"}]
      },
      {
        "path": "%[1]s/nginx_error.log",
        "is_json": false, "is_glob": false, "read_from_beginning": true,
        "outputs": ["test-output"],
        "multiline": {
          "start_pattern": "^[0-9]{4}/[0-9]{2}/[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} ",
          "mode": "halt_before", "timeout_ms": 500
        },
        "injections": [{"field": "generator", "value": "scenario-7-multiline-nginx-error"}]
      },
      {
        "path": "%[1]s/timeout.log",
        "is_json": false, "is_glob": false, "read_from_beginning": true,
        "outputs": ["test-output"],
        "multiline": {
          "start_pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}:[0-9]{2}",
          "mode": "halt_before", "timeout_ms": 500
        },
        "injections": [{"field": "generator", "value": "scenario-8-multiline-timeout"}]
      },
      {
        "path": "%[1]s/truncate.log",
        "is_json": false, "is_glob": false, "read_from_beginning": true,
        "outputs": ["test-output"],
        "multiline": {
          "start_pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}:[0-9]{2}",
          "mode": "halt_before", "timeout_ms": 500, "max_lines": 3
        },
        "injections": [{"field": "generator", "value": "scenario-9-multiline-truncation"}]
      }
    ]
  },
  "output": {
    "pubsub": [
      {"name": "test-output", "project": "%[2]s", "topic": "%[3]s", "auth_file": ""}
    ]
  }
}`

	path := filepath.Join(dir, "config.json")
	body := fmt.Sprintf(tmpl, dir, integrationProject, integrationTopic, globInterval)
	if err := os.WriteFile(path, []byte(body), 0644); err != nil {
		t.Fatalf("could not write config: %v", err)
	}
	return path
}

func buildFling(t *testing.T, dir string) string {
	t.Helper()

	bin := filepath.Join(dir, "fling_test_bin")
	cmd := exec.Command("go", "build", "-o", bin, ".")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("could not build fling: %v\n%s", err, out)
	}
	return bin
}

// appendLines appends to a fixture mid-run, which is how the tail-mode scenarios
// get lines that arrive after fling has seeked to end.
func appendLines(t *testing.T, path string, lines ...string) {
	t.Helper()

	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("could not open %s for append: %v", path, err)
	}
	defer file.Close()

	for _, line := range lines {
		if _, err := file.WriteString(line + "\n"); err != nil {
			t.Fatalf("could not append to %s: %v", path, err)
		}
	}
}

func TestPubSubIntegration(t *testing.T) {
	launcher := requirePrereqs(t)

	dir := t.TempDir()
	writeFixtures(t, dir)
	configPath := writeConfig(t, dir)
	bin := buildFling(t, dir)

	host := startEmulator(t, launcher, freePort(t))
	t.Setenv("PUBSUB_EMULATOR_HOST", host)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := pubsub.NewClient(ctx, integrationProject)
	if err != nil {
		t.Fatalf("could not create pubsub client against emulator %s: %v", host, err)
	}
	defer client.Close()

	topic, err := client.CreateTopic(ctx, integrationTopic)
	if err != nil {
		t.Fatalf("could not create topic %q: %v", integrationTopic, err)
	}
	sub, err := client.CreateSubscription(ctx, integrationSub, pubsub.SubscriptionConfig{Topic: topic})
	if err != nil {
		t.Fatalf("could not create subscription %q: %v", integrationSub, err)
	}
	sub.ReceiveSettings.MaxOutstandingMessages = 100

	// Subscribe before fling starts so nothing published during startup is lost.
	got := &collector{received: make(map[string][]map[string]interface{})}
	receiveDone := make(chan error, 1)
	go func() {
		receiveDone <- sub.Receive(ctx, func(_ context.Context, msg *pubsub.Message) {
			msg.Ack()

			var data map[string]interface{}
			if err := json.Unmarshal(msg.Data, &data); err != nil {
				t.Logf("skipping unparseable message: %v", err)
				return
			}
			gen, _ := data["generator"].(string)
			got.add(gen, data)
		})
	}()

	flingOut := &safeBuffer{}
	fling := exec.Command(bin, "-c", configPath, "-d")
	fling.Env = append(os.Environ(), "PUBSUB_EMULATOR_HOST="+host)
	fling.Stdout = flingOut
	fling.Stderr = flingOut
	if err := fling.Start(); err != nil {
		t.Fatalf("could not start fling: %v", err)
	}
	t.Cleanup(func() {
		if fling.Process != nil {
			_ = fling.Process.Kill()
			_ = fling.Wait()
		}
	})

	// Everything above ran before fling started, so each fixture's pre-existing
	// content is what a read_from_beginning source must pick up and what a
	// tail-mode source must skip. Everything below is appended while fling is
	// running - that split is the point of these scenarios, not an artifact of
	// how the harness is written, so nothing here may append before the
	// corresponding tail is established.
	for _, name := range []string{
		"static.log", "static_beginning.log", "json.log", "timeout.log",
		"mysql_error.log", "mysql_slow.log", "nginx_error.log", "truncate.log",
	} {
		path := filepath.Join(dir, name)
		if !waitFor(tailReadyTimeout, func() bool {
			return strings.Contains(flingOut.String(), path)
		}) {
			t.Fatalf("fling never reported tailing %s within %s\nfling output:\n%s",
				path, tailReadyTimeout, flingOut.String())
		}
	}

	// The publish path is up once fling's own init message lands.
	if !waitFor(tailReadyTimeout, func() bool { return got.total > 0 }) {
		t.Fatalf("fling's Pub/Sub init message never arrived within %s\nfling output:\n%s",
			tailReadyTimeout, flingOut.String())
	}

	// "Adding tail for file" is logged when the worker goroutine is spawned, but
	// a tail-mode source does not seek to end until inside that goroutine. The
	// log line is therefore a lower bound, not proof the seek finished, and
	// appending inside that window would put lines ahead of the seek point where
	// they are skipped. This is the one wait that cannot be replaced by a signal
	// the process actually emits.
	time.Sleep(tailSettleDelay)

	appendLines(t, filepath.Join(dir, "static.log"), "static-appended-1", "static-appended-2")
	appendLines(t, filepath.Join(dir, "static_beginning.log"), "beginning-appended-1", "beginning-appended-2")

	// Scenario 3: create the file empty so the glob watcher discovers and tails
	// it, and only append once it is actually being followed - otherwise the
	// lines land before the seek-to-end and never ship.
	globFile := filepath.Join(dir, "glob", "app.log")
	if err := os.WriteFile(globFile, nil, 0644); err != nil {
		t.Fatalf("could not create glob file: %v", err)
	}
	if !waitFor(globDiscoverTimeout, func() bool {
		return strings.Contains(flingOut.String(), globFile)
	}) {
		t.Fatalf("glob watcher never discovered %s within %s (glob_interval=%ds)\nfling output:\n%s",
			globFile, globDiscoverTimeout, globInterval, flingOut.String())
	}
	appendLines(t, globFile, "glob-appended-1", "glob-appended-2")

	appendLines(t, filepath.Join(dir, "glob_beginning", "app.log"), "glob-beginning-appended-1")
	appendLines(t, filepath.Join(dir, "json.log"),
		`{"message":"json-appended-1","level":"error","service":"test"}`)
	// Scenario 8: a record start with nothing after it, so only the idle timeout
	// can flush it.
	appendLines(t, filepath.Join(dir, "timeout.log"),
		"2026-07-28T10:23:48.000000Z 0 [Note] lone record awaiting timeout flush")

	expectations := integrationExpectations()

	// Return as soon as every scenario has its full complement rather than
	// sleeping a fixed interval, then keep draining briefly so an over-publishing
	// source still fails its exact-count check.
	complete := waitFor(messageWaitTimeout, func() bool {
		counts := got.counts()
		for _, exp := range expectations {
			if counts[exp.Generator] < exp.ExpectedCount {
				return false
			}
		}
		return true
	})
	if !complete {
		counts := got.counts()
		var missing []string
		for _, exp := range expectations {
			if counts[exp.Generator] < exp.ExpectedCount {
				missing = append(missing, fmt.Sprintf("  %s: got %d, want %d",
					exp.Generator, counts[exp.Generator], exp.ExpectedCount))
			}
		}
		t.Fatalf("timed out after %s waiting for messages; short by:\n%s\nfling output:\n%s",
			messageWaitTimeout, strings.Join(missing, "\n"), flingOut.String())
	}
	time.Sleep(settleDelay)

	cancel()
	if err := <-receiveDone; err != nil && ctx.Err() == nil {
		t.Fatalf("subscription receive failed: %v", err)
	}

	t.Logf("received %d messages across %d generators", got.total, len(got.counts()))

	for _, exp := range expectations {
		t.Run(exp.Generator, func(t *testing.T) {
			verify(t, exp, got.forGenerator(exp.Generator))
		})
	}
}

// verify asserts one scenario's expectations. Failures are collected rather than
// fatal so a single run reports everything wrong with a scenario at once.
func verify(t *testing.T, exp expectation, msgs []map[string]interface{}) {
	t.Helper()
	t.Logf("%s", exp.Description)

	defer func() {
		if t.Failed() {
			for i, msg := range msgs {
				data, _ := json.Marshal(msg)
				t.Logf("  received [%d] %s", i, data)
			}
		}
	}()

	if len(msgs) != exp.ExpectedCount {
		t.Errorf("got %d messages, want %d", len(msgs), exp.ExpectedCount)
	}

	messageText := func(msg map[string]interface{}) string {
		text, _ := msg["message"].(string)
		return text
	}

	for _, req := range exp.RequiredLines {
		found := false
		for _, msg := range msgs {
			if strings.Contains(messageText(msg), req) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("required line %q not found", req)
		}
	}

	for _, forbidden := range exp.ForbiddenLines {
		for _, msg := range msgs {
			if strings.Contains(messageText(msg), forbidden) {
				t.Errorf("forbidden line %q was present", forbidden)
				break
			}
		}
	}

	// Each group must land in a SINGLE message, which is what proves the record
	// was assembled rather than shredded across events.
	for _, group := range exp.RequiredJoined {
		joined := false
		for _, msg := range msgs {
			text := messageText(msg)
			all := true
			for _, part := range group {
				if !strings.Contains(text, part) {
					all = false
					break
				}
			}
			if all {
				joined = true
				break
			}
		}
		if !joined {
			t.Errorf("no single message contained all of %q (record was not assembled)", group)
		}
	}

	if exp.RequiredFlag != "" {
		found := false
		for _, msg := range msgs {
			if hasLogFlag(msg, exp.RequiredFlag) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("no message carried log.flags %q", exp.RequiredFlag)
		}
	}

	if exp.RequiredFlagOnAll != "" {
		for i, msg := range msgs {
			if !hasLogFlag(msg, exp.RequiredFlagOnAll) {
				t.Errorf("message [%d] is missing log.flags %q; part of a cut-short record arrived looking whole",
					i, exp.RequiredFlagOnAll)
				break
			}
		}
	}

	if exp.CheckLineCount {
		for _, msg := range msgs {
			want := strings.Count(messageText(msg), "\n") + 1

			raw, ok := msg["fling.multiline.line_count"]
			if !ok {
				t.Error("missing fling.multiline.line_count on a multiline source")
				break
			}
			// JSON numbers decode to float64.
			count, ok := raw.(float64)
			if !ok {
				t.Errorf("fling.multiline.line_count is %T, want a number", raw)
				break
			}
			if int(count) != want {
				t.Errorf("fling.multiline.line_count = %d but message holds %d lines", int(count), want)
				break
			}
		}
	}

	for i, msg := range msgs {
		for _, field := range []string{"fling.source", "@timestamp", "generator"} {
			if _, ok := msg[field]; !ok {
				t.Errorf("message [%d] is missing the %s field", i, field)
			}
		}
	}

	if exp.IsJSON {
		for _, msg := range msgs {
			if _, ok := msg["level"]; !ok {
				t.Error("JSON scenario: missing original 'level' field")
				break
			}
		}
	}
}
