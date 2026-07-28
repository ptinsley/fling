package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
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

func main() {
	project := flag.String("project", "test-project", "GCP project ID")
	subscription := flag.String("subscription", "fling-test-sub", "Pub/Sub subscription name")
	emulatorHost := flag.String("emulator-host", "", "Pub/Sub emulator host:port")
	flag.Parse()

	if *emulatorHost != "" {
		os.Setenv("PUBSUB_EMULATOR_HOST", *emulatorHost)
	}

	expectations := []expectation{
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

	// Pull messages from subscription
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := pubsub.NewClient(ctx, *project)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: Could not create pubsub client: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	sub := client.Subscription(*subscription)
	sub.ReceiveSettings.MaxOutstandingMessages = 100

	var mu sync.Mutex
	// Map of generator -> list of received messages
	received := make(map[string][]map[string]interface{})
	messageCount := 0

	// Use a timer that resets on each message; when it expires we're done
	silenceTimer := time.NewTimer(10 * time.Second)

	go func() {
		<-silenceTimer.C
		cancel()
	}()

	err = sub.Receive(ctx, func(_ context.Context, msg *pubsub.Message) {
		msg.Ack()

		var data map[string]interface{}
		if jsonErr := json.Unmarshal(msg.Data, &data); jsonErr != nil {
			fmt.Fprintf(os.Stderr, "WARNING: Could not unmarshal message: %v\n", jsonErr)
			return
		}

		gen, _ := data["generator"].(string)

		mu.Lock()
		received[gen] = append(received[gen], data)
		messageCount++
		mu.Unlock()

		// Reset silence timer
		silenceTimer.Reset(10 * time.Second)
	})

	if err != nil && ctx.Err() == nil {
		fmt.Fprintf(os.Stderr, "FAIL: Receive error: %v\n", err)
		os.Exit(1)
	}

	mu.Lock()
	defer mu.Unlock()

	fmt.Printf("\nReceived %d total messages across %d generators\n\n", messageCount, len(received))

	// Print all generators we saw (for debugging)
	for gen, msgs := range received {
		fmt.Printf("  generator=%q: %d messages\n", gen, len(msgs))
	}
	fmt.Println()

	// Validate expectations
	allPassed := true
	for _, exp := range expectations {
		msgs := received[exp.Generator]
		passed := true
		var failures []string

		// Check count
		if len(msgs) != exp.ExpectedCount {
			passed = false
			failures = append(failures, fmt.Sprintf("expected %d messages, got %d", exp.ExpectedCount, len(msgs)))
		}

		// Check required lines
		for _, req := range exp.RequiredLines {
			found := false
			for _, msg := range msgs {
				msgText, _ := msg["message"].(string)
				if strings.Contains(msgText, req) {
					found = true
					break
				}
			}
			if !found {
				passed = false
				failures = append(failures, fmt.Sprintf("required line %q not found", req))
			}
		}

		// Check forbidden lines
		for _, forbidden := range exp.ForbiddenLines {
			for _, msg := range msgs {
				msgText, _ := msg["message"].(string)
				if strings.Contains(msgText, forbidden) {
					passed = false
					failures = append(failures, fmt.Sprintf("forbidden line %q was present", forbidden))
					break
				}
			}
		}

		// Check that each group of substrings landed in a SINGLE message
		for _, group := range exp.RequiredJoined {
			joined := false
			for _, msg := range msgs {
				msgText, _ := msg["message"].(string)

				all := true
				for _, part := range group {
					if !strings.Contains(msgText, part) {
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
				passed = false
				failures = append(failures,
					fmt.Sprintf("no single message contained all of %q (record was not assembled)", group))
			}
		}

		// Check the ECS log.flags marker
		if exp.RequiredFlag != "" {
			found := false
			for _, msg := range msgs {
				if hasLogFlag(msg, exp.RequiredFlag) {
					found = true
					break
				}
			}

			if !found {
				passed = false
				failures = append(failures,
					fmt.Sprintf("no message carried log.flags %q", exp.RequiredFlag))
			}
		}

		if exp.RequiredFlagOnAll != "" {
			for i, msg := range msgs {
				if !hasLogFlag(msg, exp.RequiredFlagOnAll) {
					passed = false
					failures = append(failures,
						fmt.Sprintf("message [%d] is missing log.flags %q; part of a cut-short record arrived looking whole",
							i, exp.RequiredFlagOnAll))
					break
				}
			}
		}

		// Check the multiline line count matches the assembled message
		if exp.CheckLineCount {
			for _, msg := range msgs {
				msgText, _ := msg["message"].(string)
				want := strings.Count(msgText, "\n") + 1

				raw, ok := msg["fling.multiline.line_count"]
				if !ok {
					passed = false
					failures = append(failures, "missing fling.multiline.line_count on a multiline source")
					break
				}

				// JSON numbers decode to float64
				count, ok := raw.(float64)
				if !ok {
					passed = false
					failures = append(failures,
						fmt.Sprintf("fling.multiline.line_count is %T, want a number", raw))
					break
				}

				if int(count) != want {
					passed = false
					failures = append(failures,
						fmt.Sprintf("fling.multiline.line_count = %d but message holds %d lines", int(count), want))
					break
				}
			}
		}

		// Check standard fields present on all messages
		for _, msg := range msgs {
			if _, ok := msg["fling.source"]; !ok {
				passed = false
				failures = append(failures, "missing fling.source field")
				break
			}
			if _, ok := msg["@timestamp"]; !ok {
				passed = false
				failures = append(failures, "missing @timestamp field")
				break
			}
			if _, ok := msg["generator"]; !ok {
				passed = false
				failures = append(failures, "missing generator injection field")
				break
			}
		}

		// JSON scenario: check that original JSON fields are preserved
		if exp.IsJSON {
			for _, msg := range msgs {
				if _, ok := msg["level"]; !ok {
					passed = false
					failures = append(failures, "JSON scenario: missing original 'level' field")
					break
				}
			}
		}

		if passed {
			fmt.Printf("PASS: %s (%s)\n", exp.Generator, exp.Description)
		} else {
			allPassed = false
			fmt.Printf("FAIL: %s (%s)\n", exp.Generator, exp.Description)
			for _, f := range failures {
				fmt.Printf("      - %s\n", f)
			}
			// Print received messages for debugging
			fmt.Printf("      received messages:\n")
			for i, msg := range msgs {
				data, _ := json.Marshal(msg)
				fmt.Printf("        [%d] %s\n", i, string(data))
			}
		}
	}

	fmt.Println()
	if allPassed {
		fmt.Println("ALL TESTS PASSED")
		os.Exit(0)
	} else {
		fmt.Println("SOME TESTS FAILED")
		os.Exit(1)
	}
}
