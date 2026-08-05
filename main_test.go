package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
)

// Timestamps handed to the aggregator. Fixed so assertions on the emitted
// @timestamp are deterministic.
var (
	baseTime = time.Date(2026, 7, 28, 10, 23, 45, 0, time.UTC)
)

func lineTimeAt(offset int) time.Time {
	return baseTime.Add(time.Duration(offset) * time.Millisecond)
}

// feed pushes every line through the aggregator and returns the events that
// completed, without any trailing flush.
func feed(t *testing.T, a *multilineAggregator, lines []string) []aggregatedEvent {
	t.Helper()

	var events []aggregatedEvent
	for i, line := range lines {
		events = append(events, a.add(line, lineTimeAt(i))...)
	}

	return events
}

// feedAndFlush pushes every line through and then lets the record go idle, which
// is what the flush ticker does in fileInWorker.
func feedAndFlush(t *testing.T, a *multilineAggregator, lines []string) []aggregatedEvent {
	t.Helper()

	events := feed(t, a, lines)
	if event, ok := a.idleFlush(lineTimeAt(len(lines)).Add(a.timeout)); ok {
		events = append(events, event)
	}

	return events
}

func mustAggregator(t *testing.T, config FlingMultiline) *multilineAggregator {
	t.Helper()

	a, err := newMultilineAggregator(config)
	if err != nil {
		t.Fatalf("newMultilineAggregator(%+v) returned error: %v", config, err)
	}

	return a
}

func assertTexts(t *testing.T, got []aggregatedEvent, want []string) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("got %d events, want %d\ngot:  %q\nwant: %q",
			len(got), len(want), eventTexts(got), want)
	}

	for i := range want {
		if got[i].Text != want[i] {
			t.Errorf("event %d text:\n got: %q\nwant: %q", i, got[i].Text, want[i])
		}
	}
}

func eventTexts(events []aggregatedEvent) []string {
	texts := make([]string, 0, len(events))
	for _, event := range events {
		texts = append(texts, event.Text)
	}

	return texts
}

// --- Modes -----------------------------------------------------------------

func TestAggregatorModes(t *testing.T) {
	tests := []struct {
		name   string
		config FlingMultiline
		lines  []string
		want   []string
	}{
		{
			name: "halt_before groups until the next start line",
			config: FlingMultiline{
				StartPattern: `^\d{4}-`,
				Mode:         multilineModeHaltBefore,
				TimeoutMS:    1000,
			},
			lines: []string{
				"2026-07-28 first",
				"  continuation a",
				"  continuation b",
				"2026-07-28 second",
				"  continuation c",
			},
			want: []string{
				"2026-07-28 first\n  continuation a\n  continuation b",
				"2026-07-28 second\n  continuation c",
			},
		},
		{
			// Regression: the halting line must open the next record even though it
			// does not match start_pattern. Previously it was orphaned, and so was
			// everything that should have joined it.
			name: "halt_before with condition_pattern broader than start_pattern",
			config: FlingMultiline{
				StartPattern:     `^\d{4}-\d{2}-\d{2}`,
				ConditionPattern: `^\d{4}-\d{2}-\d{2}|^===`,
				Mode:             multilineModeHaltBefore,
				TimeoutMS:        1000,
			},
			lines: []string{
				"2026-07-28 first",
				"  continuation a",
				"=== SECTION BREAK",
				"  section continuation",
			},
			want: []string{
				"2026-07-28 first\n  continuation a",
				"=== SECTION BREAK\n  section continuation",
			},
		},
		{
			name: "continue_through groups consecutive matching lines",
			config: FlingMultiline{
				StartPattern:     `^\d{4}-`,
				ConditionPattern: `^\s`,
				Mode:             multilineModeContinueThrough,
				TimeoutMS:        1000,
			},
			lines: []string{
				"2026-07-28 first",
				"  indented a",
				"  indented b",
				"2026-07-28 second",
			},
			want: []string{
				"2026-07-28 first\n  indented a\n  indented b",
				"2026-07-28 second",
			},
		},
		{
			name: "continue_past joins the line after each marker",
			config: FlingMultiline{
				StartPattern:     `^query`,
				ConditionPattern: `\\$`,
				Mode:             multilineModeContinuePast,
				TimeoutMS:        1000,
			},
			lines: []string{
				`query one \`,
				`still one \`,
				`end of one`,
				`query two`,
			},
			want: []string{
				"query one \\\nstill one \\\nend of one",
				"query two",
			},
		},
		{
			name: "halt_with closes on the terminator, inclusive",
			config: FlingMultiline{
				StartPattern:     `^BEGIN`,
				ConditionPattern: `;$`,
				Mode:             multilineModeHaltWith,
				TimeoutMS:        1000,
			},
			lines: []string{
				"BEGIN",
				"  SELECT 1",
				"COMMIT;",
				"BEGIN",
				"  SELECT 2",
				"COMMIT;",
			},
			want: []string{
				"BEGIN\n  SELECT 1\nCOMMIT;",
				"BEGIN\n  SELECT 2\nCOMMIT;",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			a := mustAggregator(t, test.config)
			assertTexts(t, feedAndFlush(t, a, test.lines), test.want)
		})
	}
}

// --- Real log shapes -------------------------------------------------------

// A MySQL 8.0 crash backtrace is the motivating case: one logical record over
// many physical lines, only the first of which carries a timestamp.
func TestAggregatorMySQLErrorLogCrashBacktrace(t *testing.T) {
	a := mustAggregator(t, FlingMultiline{
		StartPattern: `^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}`,
		Mode:         multilineModeHaltBefore,
		TimeoutMS:    2000,
	})

	lines := []string{
		"2026-07-28T10:23:45.123456Z 0 [ERROR] [MY-013183] [InnoDB] Assertion failure: trx0trx.cc:1234",
		"InnoDB: We intentionally generate a memory trap.",
		"InnoDB: Submit a detailed bug report to http://bugs.mysql.com.",
		"10:23:45 UTC - mysqld got signal 6 ;",
		"Thread pointer: 0x7f8b40000f00",
		"Attempting backtrace. You can use the following information to find out",
		"stack_bottom = 7f8b5c1f9d90 thread_stack 0x46000",
		"/usr/sbin/mysqld(my_print_stacktrace(unsigned char const*, unsigned long)+0x41) [0x21f0b41]",
		"2026-07-28T10:23:46.000000Z 0 [System] [MY-010910] [Server] /usr/sbin/mysqld: Shutdown complete.",
	}

	events := feedAndFlush(t, a, lines)

	if len(events) != 2 {
		t.Fatalf("got %d events, want 2 (the crash record and the shutdown line)\ngot: %q",
			len(events), eventTexts(events))
	}

	crash := events[0]
	if got := strings.Count(crash.Text, "\n") + 1; got != 8 {
		t.Errorf("crash record joined %d lines, want 8:\n%s", got, crash.Text)
	}
	if !strings.Contains(crash.Text, "Assertion failure") ||
		!strings.Contains(crash.Text, "my_print_stacktrace") {
		t.Errorf("crash record lost its head or tail:\n%s", crash.Text)
	}
	if crash.Truncated {
		t.Error("crash record marked truncated but it is well under the limits")
	}
	// The record is stamped when it opened, not when the next timestamp closed it.
	if !crash.Time.Equal(lineTimeAt(0)) {
		t.Errorf("crash record @timestamp = %v, want first line time %v", crash.Time, lineTimeAt(0))
	}

	if !strings.Contains(events[1].Text, "Shutdown complete") {
		t.Errorf("second event should be the shutdown line, got:\n%s", events[1].Text)
	}
}

// The slow query log is the case where every record is multi-line. It anchors on
// "# User@Host:" rather than "# Time:" because MySQL omits "# Time:" for queries
// landing in the same second as the previous entry - anchoring on it would merge
// those queries into the record above.
func TestAggregatorMySQLSlowQueryLog(t *testing.T) {
	a := mustAggregator(t, FlingMultiline{
		StartPattern: `^# User@Host:`,
		Mode:         multilineModeHaltBefore,
		TimeoutMS:    2000,
	})

	lines := []string{
		"# Time: 2026-07-28T10:23:45.123456Z",
		"# User@Host: app[app] @ 10.0.0.5 [10.0.0.5]  Id: 12345",
		"# Query_time: 2.345678  Lock_time: 0.000123 Rows_sent: 1  Rows_examined: 1000000",
		"SET timestamp=1785316225;",
		"SELECT * FROM orders WHERE customer_id = 42",
		"  AND created_at > '2026-01-01';",
		// No "# Time:" here - same second as the entry above. This is the case that
		// breaks a "# Time:"-anchored pattern.
		"# User@Host: app[app] @ 10.0.0.6 [10.0.0.6]  Id: 12346",
		"# Query_time: 1.111  Lock_time: 0.000 Rows_sent: 5  Rows_examined: 900",
		"SET timestamp=1785316225;",
		"SELECT COUNT(*) FROM line_items;",
		"# Time: 2026-07-28T10:23:47.000000Z",
		"# User@Host: app[app] @ 10.0.0.7 [10.0.0.7]  Id: 12347",
		"# Query_time: 3.5  Lock_time: 0.0 Rows_sent: 1  Rows_examined: 10",
		"SELECT SLEEP(3);",
	}

	events := feedAndFlush(t, a, lines)

	want := []string{
		// Leading "# Time:" arrives before any record is open, so it ships alone.
		lines[0],
		strings.Join(lines[1:6], "\n"),
		// The trailing "# Time:" of the next entry attaches to this record. Same
		// behaviour as Filebeat's mysql module; the query's own header and SQL stay
		// together, which is what matters.
		strings.Join(lines[6:11], "\n"),
		strings.Join(lines[11:14], "\n"),
	}
	assertTexts(t, events, want)

	// The critical property: the entry that omitted "# Time:" kept its own header,
	// timing and SQL in a single record.
	sameSecond := events[2].Text
	for _, required := range []string{"Id: 12346", "Query_time: 1.111", "SELECT COUNT(*)"} {
		if !strings.Contains(sameSecond, required) {
			t.Errorf("same-second entry lost %q:\n%s", required, sameSecond)
		}
	}
	if strings.Contains(sameSecond, "Id: 12345") || strings.Contains(sameSecond, "Id: 12347") {
		t.Errorf("same-second entry merged with a neighbouring query:\n%s", sameSecond)
	}

	// A query whose SQL spans lines keeps all of it.
	if !strings.Contains(events[1].Text, "AND created_at") {
		t.Errorf("multi-line SQL was split:\n%s", events[1].Text)
	}
}

// nginx error.log carries app stderr passthrough, so untimestamped backtrace
// lines have to attach to the entry above them.
func TestAggregatorNginxErrorLogWithRubyBacktrace(t *testing.T) {
	a := mustAggregator(t, FlingMultiline{
		StartPattern: `^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} |\[ [EWIDC] \d{4}-\d{2}-\d{2} )`,
		Mode:         multilineModeHaltBefore,
		TimeoutMS:    1000,
	})

	lines := []string{
		`2026/07/28 10:23:45 [error] 1234#0: *567 Passenger: app error, client: 10.0.0.1`,
		`App 4242 stderr: ActiveRecord::StatementInvalid (Mysql2::Error: Lost connection)`,
		`App 4242 stderr:   from /webapp/app/models/order.rb:88:in 'total'`,
		`App 4242 stderr:   from /webapp/app/controllers/orders_controller.rb:12:in 'show'`,
		`[ E 2026-07-28 10:23:46.9999 1234/T5 age/Cor/Con.h:200 ]: Could not spawn process`,
		`  Error ID: abcdef01`,
		`2026/07/28 10:23:47 [warn] 1234#0: *568 upstream server temporarily disabled`,
	}

	events := feedAndFlush(t, a, lines)

	want := []string{
		strings.Join(lines[0:4], "\n"),
		strings.Join(lines[4:6], "\n"),
		lines[6],
	}
	assertTexts(t, events, want)
}

// --- Never drop a line -----------------------------------------------------

func TestAggregatorEmitsOrphanLineWhenAttachedMidRecord(t *testing.T) {
	a := mustAggregator(t, FlingMultiline{
		StartPattern: `^\d{4}-`,
		Mode:         multilineModeHaltBefore,
		TimeoutMS:    1000,
	})

	// fling seeks to the end of a file, so the first lines it sees are routinely
	// the middle of a record. They must still be shipped.
	events := feedAndFlush(t, a, []string{
		"  orphaned continuation",
		"  another orphan",
		"2026-07-28 a real start",
	})

	assertTexts(t, events, []string{
		"  orphaned continuation",
		"  another orphan",
		"2026-07-28 a real start",
	})
}

// --- Idle timeout ----------------------------------------------------------

func TestAggregatorIdleFlush(t *testing.T) {
	a := mustAggregator(t, FlingMultiline{
		StartPattern: `^\d{4}-`,
		Mode:         multilineModeHaltBefore,
		TimeoutMS:    1000,
	})

	if _, ok := a.idleFlush(baseTime); ok {
		t.Error("empty aggregator flushed an event; there is nothing buffered")
	}

	a.add("2026-07-28 waiting for more", baseTime)

	if _, ok := a.idleFlush(baseTime.Add(999 * time.Millisecond)); ok {
		t.Error("flushed before the timeout elapsed")
	}

	event, ok := a.idleFlush(baseTime.Add(1000 * time.Millisecond))
	if !ok {
		t.Fatal("not flushed at the timeout; the last record of an idle file would be stranded")
	}
	if event.Text != "2026-07-28 waiting for more" {
		t.Errorf("flushed text = %q", event.Text)
	}

	if _, ok := a.idleFlush(baseTime.Add(2 * time.Second)); ok {
		t.Error("second flush returned an event; the buffer was not cleared")
	}
}

// An idle flush means different things per mode: halt_before and continue_through
// records end when their continuation lines stop, but halt_with never saw its
// terminator and continue_past never saw the line its last marker promised.
func TestAggregatorIdleFlushFlagsModesThatWereCutShort(t *testing.T) {
	tests := []struct {
		name          string
		config        FlingMultiline
		lines         []string
		wantTruncated bool
	}{
		{
			name: "halt_before ends legitimately when the file goes quiet",
			config: FlingMultiline{
				StartPattern: `^\d{4}-`,
				Mode:         multilineModeHaltBefore,
				TimeoutMS:    1000,
			},
			lines:         []string{"2026-07-28 complete record", "  continuation"},
			wantTruncated: false,
		},
		{
			name: "continue_through ends legitimately when the file goes quiet",
			config: FlingMultiline{
				StartPattern:     `^\d{4}-`,
				ConditionPattern: `^\s`,
				Mode:             multilineModeContinueThrough,
				TimeoutMS:        1000,
			},
			lines:         []string{"2026-07-28 complete record", "  continuation"},
			wantTruncated: false,
		},
		{
			// The terminator never arrived, so this record is missing its tail.
			name: "halt_with is cut short without its terminator",
			config: FlingMultiline{
				StartPattern:     `^BEGIN`,
				ConditionPattern: `;$`,
				Mode:             multilineModeHaltWith,
				TimeoutMS:        1000,
			},
			lines:         []string{"BEGIN", "  SELECT 1"},
			wantTruncated: true,
		},
		{
			// The last line still carries the continuation marker, so the line it
			// promised never arrived.
			name: "continue_past is cut short after a trailing marker",
			config: FlingMultiline{
				StartPattern:     `^query`,
				ConditionPattern: `\\$`,
				Mode:             multilineModeContinuePast,
				TimeoutMS:        1000,
			},
			lines:         []string{`query one \`, `still going \`},
			wantTruncated: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			a := mustAggregator(t, test.config)
			feed(t, a, test.lines)

			event, ok := a.idleFlush(lineTimeAt(len(test.lines)).Add(a.timeout))
			if !ok {
				t.Fatal("nothing flushed; the record was not left buffered")
			}
			if event.Truncated != test.wantTruncated {
				t.Errorf("Truncated = %v, want %v for an idle %s record",
					event.Truncated, test.wantTruncated, test.config.Mode)
			}
		})
	}
}

// --- Truncation ------------------------------------------------------------

func TestAggregatorTruncatesOnMaxLines(t *testing.T) {
	a := mustAggregator(t, FlingMultiline{
		StartPattern: `^START`,
		Mode:         multilineModeHaltBefore,
		TimeoutMS:    1000,
		MaxLines:     3,
	})

	events := feedAndFlush(t, a, []string{"START", "two", "three", "four", "five"})

	// The cap cuts the record at 3 lines. What follows is the *tail* of that
	// record, not new records, so it keeps being assembled into a second chunk
	// rather than falling out as one event per line.
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2 (capped record + assembled remainder)\ngot: %q",
			len(events), eventTexts(events))
	}
	if events[0].Text != "START\ntwo\nthree" {
		t.Errorf("truncated text = %q, want the first 3 lines", events[0].Text)
	}
	if !events[0].Truncated {
		t.Error("record hit max_lines but was not marked truncated")
	}
	if events[1].Text != "four\nfive" {
		t.Errorf("remainder text = %q, want the rest of the record assembled", events[1].Text)
	}
	if !events[1].Truncated {
		t.Error("remainder of a capped record not flagged; downstream cannot tell it is a fragment")
	}

	// The cap must not leak into the next record.
	if a.truncated {
		t.Error("truncated flag survived take() and would mislabel the next record")
	}
	if a.draining {
		t.Error("drain state survived the idle flush; later records would be mislabelled")
	}
}

// A start line ends the drain: it opens a genuine new record, which is whole.
func TestAggregatorDrainEndsAtTheNextStartLine(t *testing.T) {
	a := mustAggregator(t, FlingMultiline{
		StartPattern: `^START`,
		Mode:         multilineModeHaltBefore,
		TimeoutMS:    1000,
		MaxLines:     2,
	})

	// The last record stays inside the cap so that it is whole, and so the only
	// thing that could flag it is a drain state that outlived the record it belongs to.
	events := feedAndFlush(t, a, []string{"START one", "two", "three", "START two"})

	// capped chunk, drain chunk, then the next record.
	assertTexts(t, events, []string{"START one\ntwo", "three", "START two"})

	if !events[0].Truncated || !events[1].Truncated {
		t.Errorf("both halves of the cut record must be flagged, got %v and %v",
			events[0].Truncated, events[1].Truncated)
	}
	if events[2].Truncated {
		t.Error("the record after the drain is whole and must not be flagged")
	}
}

// A quiet file also ends the drain, so a line arriving long afterwards is not
// mislabelled as the tail of a record that was cut short.
func TestAggregatorDrainEndsWhenFileGoesQuiet(t *testing.T) {
	a := mustAggregator(t, FlingMultiline{
		StartPattern: `^START`,
		Mode:         multilineModeHaltBefore,
		TimeoutMS:    1000,
		MaxLines:     2,
	})

	feed(t, a, []string{"START", "two"}) // caps and sets the drain

	if _, ok := a.idleFlush(baseTime.Add(2 * time.Second)); ok {
		t.Fatal("idleFlush emitted an event; the capped record was already taken")
	}

	events := a.add("much later, unrelated", baseTime.Add(time.Hour))
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1\ngot: %q", len(events), eventTexts(events))
	}
	if events[0].Truncated {
		t.Error("line after an idle period flagged as the tail of the capped record")
	}
}

// A record's own terminator ends the drain too. The drain tests above all use
// halt_before, which is the one mode where a record never closes on a line of its
// own, so they cannot catch drain state outliving a record that closed properly.
func TestAggregatorDrainEndsAtTheTerminator(t *testing.T) {
	a := mustAggregator(t, FlingMultiline{
		StartPattern:     `^BEGIN`,
		ConditionPattern: `^END`,
		Mode:             multilineModeHaltWith,
		TimeoutMS:        1000,
		MaxLines:         3,
	})

	events := feed(t, a, []string{
		"BEGIN", "x", "y", // the cap cuts the record here
		"z", "END", // the remainder, closed by the terminator it was waiting for
		"unrelated one", // nothing to do with the record above
		"unrelated two",
	})

	assertTexts(t, events, []string{"BEGIN\nx\ny", "z\nEND", "unrelated one", "unrelated two"})

	if !events[0].Truncated || !events[1].Truncated {
		t.Errorf("both halves of the cut record must be flagged, got %v and %v",
			events[0].Truncated, events[1].Truncated)
	}
	if events[2].Truncated || events[3].Truncated {
		t.Error("lines after the terminator flagged as the tail of the capped record")
	}
	if a.draining {
		t.Error("drain state survived the terminator; later lines would be merged into one event and mislabelled")
	}
	if len(a.buf) != 0 {
		t.Errorf("unrelated lines left buffered as %q, awaiting a terminator that is not coming", a.buf)
	}
}

// Same for continue_past, where the record closes on the first line *without* the
// continuation marker rather than on a terminator.
func TestAggregatorDrainEndsAtTheLineWithoutAMarker(t *testing.T) {
	a := mustAggregator(t, FlingMultiline{
		StartPattern:     `^query`,
		ConditionPattern: `\\$`,
		Mode:             multilineModeContinuePast,
		TimeoutMS:        1000,
		MaxLines:         2,
	})

	events := feed(t, a, []string{
		`query one \`, `still going \`, // the cap cuts the record here
		`and more \`, `last part`, // the remainder, closed by the unmarked line
		"unrelated one",
		"unrelated two",
	})

	assertTexts(t, events, []string{
		"query one \\\nstill going \\",
		"and more \\\nlast part",
		"unrelated one",
		"unrelated two",
	})

	if !events[0].Truncated || !events[1].Truncated {
		t.Errorf("both halves of the cut record must be flagged, got %v and %v",
			events[0].Truncated, events[1].Truncated)
	}
	if events[2].Truncated || events[3].Truncated {
		t.Errorf("lines after the record closed flagged as its tail, got %v and %v",
			events[2].Truncated, events[3].Truncated)
	}
	if a.draining {
		t.Error("drain state survived the closing line; later records would be mislabelled")
	}
}

// A halt_before boundary line ends the drain even though the limit that started it
// already emptied the buffer. Only reachable when condition_pattern is broader than
// start_pattern; otherwise a boundary line is also a start line and clears it anyway.
func TestAggregatorDrainEndsAtABroadConditionBoundary(t *testing.T) {
	a := mustAggregator(t, FlingMultiline{
		StartPattern:     `^START`,
		ConditionPattern: `^START|^===`,
		Mode:             multilineModeHaltBefore,
		TimeoutMS:        1000,
		MaxLines:         2,
	})

	// The cap cuts after "b" and empties the buffer. "=== section" is still this
	// mode's record boundary, so it opens a genuine new record - which is whole.
	events := feed(t, a, []string{"START a", "b", "=== section"})

	if a.draining {
		t.Error("drain state ran across the boundary line; the record it opened will be mislabelled")
	}

	event, ok := a.idleFlush(lineTimeAt(3).Add(a.timeout))
	if !ok {
		t.Fatal("the boundary line's record was not left buffered")
	}
	events = append(events, event)

	assertTexts(t, events, []string{"START a\nb", "=== section"})

	if !events[0].Truncated {
		t.Error("the capped record was not flagged")
	}
	if events[1].Truncated {
		t.Error("record opened by a boundary line flagged as the tail of the capped record")
	}
}

// A lone line meeting max_bytes is whole, not severed. Flagging it truncated is the
// inverse of the mistake that flag exists to prevent, so it gets its own.
func TestAggregatorFlagsAWholeOversizedLineSeparately(t *testing.T) {
	a := mustAggregator(t, FlingMultiline{
		StartPattern: `^START`,
		Mode:         multilineModeHaltBefore,
		TimeoutMS:    1000,
		MaxBytes:     16,
	})

	huge := "START " + strings.Repeat("x", 40)
	events := feed(t, a, []string{huge})

	if len(events) != 1 {
		t.Fatalf("got %d events, want 1\ngot: %q", len(events), eventTexts(events))
	}
	if events[0].Text != huge {
		t.Errorf("the line was altered: %q", events[0].Text)
	}
	if events[0].Truncated {
		t.Error("a whole line flagged truncated; nothing was cut from it")
	}
	if !events[0].Oversized {
		t.Error("not flagged oversized; downstream cannot tell it overran max_bytes")
	}
}

// The limit still starts a drain, so what follows an oversized head is stitched to it
// rather than shredded - and those chunks really were cut, so they are truncated.
func TestAggregatorDrainsAfterAnOversizedLine(t *testing.T) {
	a := mustAggregator(t, FlingMultiline{
		StartPattern: `^START`,
		Mode:         multilineModeHaltBefore,
		TimeoutMS:    1000,
		MaxBytes:     16,
	})

	head := "START " + strings.Repeat("x", 40)
	events := feedAndFlush(t, a, []string{head, "  cont a", "  cont b"})

	assertTexts(t, events, []string{head, "  cont a\n  cont b"})

	if events[0].Truncated || !events[0].Oversized {
		t.Errorf("head: Truncated=%v Oversized=%v, want false and true",
			events[0].Truncated, events[0].Oversized)
	}
	if !events[1].Truncated {
		t.Error("the tail of a record cut by max_bytes must be flagged truncated")
	}
	if events[1].Oversized {
		t.Error("tail chunk holds two lines, so max_bytes cut it; it is not one oversized line")
	}
}

// The two flags are independent, not exclusive. A lone oversized line that is also
// the tail of a record an earlier limit cut carries both, because both are true of it.
func TestAggregatorCombinesBothFlagsOnAnOversizedDrainChunk(t *testing.T) {
	a := mustAggregator(t, FlingMultiline{
		StartPattern: `^START`,
		Mode:         multilineModeHaltBefore,
		TimeoutMS:    1000,
		MaxBytes:     16,
	})

	long := strings.Repeat("b", 18)
	events := feed(t, a, []string{"START a", long, strings.Repeat("c", 18)})

	if len(events) != 2 {
		t.Fatalf("got %d events, want 2\ngot: %q", len(events), eventTexts(events))
	}
	if !events[0].Truncated || events[0].Oversized {
		t.Errorf("cut record: Truncated=%v Oversized=%v, want true and false",
			events[0].Truncated, events[0].Oversized)
	}
	if !events[1].Truncated {
		t.Error("drain chunk lost its truncated flag; it is still the tail of a cut record")
	}
	if !events[1].Oversized {
		t.Error("drain chunk is one line over max_bytes and must say so too")
	}
}

// max_lines of 1 genuinely cuts every record short, so those chunks stay truncated
// even though each is a single line.
func TestAggregatorMaxLinesOfOneStaysTruncated(t *testing.T) {
	a := mustAggregator(t, FlingMultiline{
		StartPattern: `^START`,
		Mode:         multilineModeHaltBefore,
		TimeoutMS:    1000,
		MaxLines:     1,
	})

	events := feed(t, a, []string{"START a", "b"})

	if len(events) != 2 {
		t.Fatalf("got %d events, want 2\ngot: %q", len(events), eventTexts(events))
	}
	for i, event := range events {
		if !event.Truncated {
			t.Errorf("event %d (%q) not flagged truncated", i, event.Text)
		}
		if event.Oversized {
			t.Errorf("event %d (%q) flagged oversized, but max_lines is what cut it", i, event.Text)
		}
	}
}

func TestAggregatorTruncatesOnMaxBytes(t *testing.T) {
	a := mustAggregator(t, FlingMultiline{
		StartPattern: `^START`,
		Mode:         multilineModeHaltBefore,
		TimeoutMS:    1000,
		MaxBytes:     32,
	})

	events := feed(t, a, []string{
		"START",
		strings.Repeat("x", 20),
		strings.Repeat("y", 20),
		strings.Repeat("z", 20),
	})

	if len(events) == 0 {
		t.Fatal("no event emitted; a runaway record would grow without bound")
	}
	if !events[0].Truncated {
		t.Error("record hit max_bytes but was not marked truncated")
	}

	// The limit is checked after a line is appended, so the record overshoots by
	// exactly the line that crossed it - "START\n" plus two 20-byte lines - and
	// nothing more. Asserting the exact size is what catches an accounting drift.
	want := "START\n" + strings.Repeat("x", 20) + "\n" + strings.Repeat("y", 20)
	if events[0].Text != want {
		t.Errorf("truncated record = %q (%d bytes), want %q (%d bytes)",
			events[0].Text, len(events[0].Text), want, len(want))
	}
}

// bytes has to track the joined length exactly, since max_bytes is what keeps a
// record below the ceiling of whatever consumes the topic.
func TestAggregatorByteCountMatchesJoinedLength(t *testing.T) {
	a := mustAggregator(t, FlingMultiline{
		StartPattern: `^START`,
		Mode:         multilineModeHaltBefore,
		TimeoutMS:    1000,
	})

	feed(t, a, []string{"START", "second line", "third"})

	if got, want := a.bytes, len(strings.Join(a.buf, "\n")); got != want {
		t.Errorf("bytes = %d, want %d (the length of the joined record)", got, want)
	}
}

// --- Construction and validation -------------------------------------------

func TestNewMultilineAggregatorValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  FlingMultiline
		wantErr string
	}{
		{
			name:    "missing mode",
			config:  FlingMultiline{StartPattern: `^x`, TimeoutMS: 1000},
			wantErr: "mode is required",
		},
		{
			name:    "unknown mode",
			config:  FlingMultiline{StartPattern: `^x`, Mode: "sometimes", TimeoutMS: 1000},
			wantErr: "unknown mode",
		},
		{
			name:    "missing start_pattern",
			config:  FlingMultiline{Mode: multilineModeHaltBefore, TimeoutMS: 1000},
			wantErr: "start_pattern is required",
		},
		{
			name:    "uncompilable start_pattern",
			config:  FlingMultiline{StartPattern: `^(`, Mode: multilineModeHaltBefore, TimeoutMS: 1000},
			wantErr: "start_pattern",
		},
		{
			name: "uncompilable condition_pattern",
			config: FlingMultiline{
				StartPattern: `^x`, ConditionPattern: `^(`,
				Mode: multilineModeHaltBefore, TimeoutMS: 1000,
			},
			wantErr: "condition_pattern",
		},
		{
			name:    "zero timeout",
			config:  FlingMultiline{StartPattern: `^x`, Mode: multilineModeHaltBefore},
			wantErr: "timeout_ms must be greater than 0",
		},
		{
			name:    "continue_through without condition_pattern",
			config:  FlingMultiline{StartPattern: `^x`, Mode: multilineModeContinueThrough, TimeoutMS: 1000},
			wantErr: "condition_pattern is required",
		},
		{
			name:    "continue_past without condition_pattern",
			config:  FlingMultiline{StartPattern: `^x`, Mode: multilineModeContinuePast, TimeoutMS: 1000},
			wantErr: "condition_pattern is required",
		},
		{
			// Defaulting condition_pattern to start_pattern here would close every
			// record on the line that opened it - a silent one-line-per-record
			// no-op that looks like a working config.
			name:    "halt_with without condition_pattern",
			config:  FlingMultiline{StartPattern: `^x`, Mode: multilineModeHaltWith, TimeoutMS: 1000},
			wantErr: "condition_pattern is required",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := newMultilineAggregator(test.config)
			if err == nil {
				t.Fatalf("config %+v accepted, want error containing %q", test.config, test.wantErr)
			}
			if !strings.Contains(err.Error(), test.wantErr) {
				t.Errorf("error = %q, want it to contain %q", err, test.wantErr)
			}
		})
	}
}

func TestNewMultilineAggregatorDefaults(t *testing.T) {
	a := mustAggregator(t, FlingMultiline{
		StartPattern: `^\d{4}-`,
		Mode:         multilineModeHaltBefore,
		TimeoutMS:    1500,
	})

	// condition_pattern defaults to start_pattern for the halt modes.
	if a.cond.String() != `^\d{4}-` {
		t.Errorf("condition_pattern = %q, want it defaulted to start_pattern", a.cond)
	}
	if a.maxLines != defaultMultilineMaxLines {
		t.Errorf("maxLines = %d, want %d", a.maxLines, defaultMultilineMaxLines)
	}
	if a.maxBytes != defaultMultilineMaxBytes {
		t.Errorf("maxBytes = %d, want %d", a.maxBytes, defaultMultilineMaxBytes)
	}
	if a.timeout != 1500*time.Millisecond {
		t.Errorf("timeout = %v, want 1.5s", a.timeout)
	}
}

func TestFlushIntervalStaysWithinBounds(t *testing.T) {
	tests := []struct {
		timeoutMS int
		want      time.Duration
	}{
		{timeoutMS: 50, want: minFlushCheckInterval},    // half of 50ms floors
		{timeoutMS: 200, want: 100 * time.Millisecond},  // half of the timeout
		{timeoutMS: 60000, want: maxFlushCheckInterval}, // half of 60s caps
	}

	for _, test := range tests {
		a := mustAggregator(t, FlingMultiline{
			StartPattern: `^x`,
			Mode:         multilineModeHaltBefore,
			TimeoutMS:    test.timeoutMS,
		})

		if got := a.flushInterval(); got != test.want {
			t.Errorf("timeout_ms=%d: flushInterval() = %v, want %v", test.timeoutMS, got, test.want)
		}
	}
}

// --- Config validation wiring ----------------------------------------------

func TestValidateConfig(t *testing.T) {
	good := FlingMultiline{
		StartPattern: `^\d{4}-`,
		Mode:         multilineModeHaltBefore,
		TimeoutMS:    1000,
	}
	bad := FlingMultiline{
		StartPattern: `^(`,
		Mode:         multilineModeHaltBefore,
		TimeoutMS:    1000,
	}

	t.Run("no multiline blocks", func(t *testing.T) {
		config := FlingConfig{Input: FlingInput{Files: []FlingInFile{{Path: "/a.log"}}}}
		if err := validateConfig(config); err != nil {
			t.Errorf("validateConfig returned %v, want nil", err)
		}
	})

	t.Run("valid multiline block", func(t *testing.T) {
		config := FlingConfig{Input: FlingInput{Files: []FlingInFile{
			{Path: "/a.log", Multiline: &good},
		}}}
		if err := validateConfig(config); err != nil {
			t.Errorf("validateConfig returned %v, want nil", err)
		}
	})

	t.Run("bad block fails at startup naming the file", func(t *testing.T) {
		config := FlingConfig{Input: FlingInput{Files: []FlingInFile{
			{Path: "/a.log", Multiline: &good},
			{Path: "/broken.log", Multiline: &bad},
		}}}

		err := validateConfig(config)
		if err == nil {
			t.Fatal("validateConfig accepted an uncompilable pattern")
		}
		if !strings.Contains(err.Error(), "/broken.log") {
			t.Errorf("error %q does not name the offending file", err)
		}
	})

	t.Run("checks the top level files list too", func(t *testing.T) {
		config := FlingConfig{Files: []FlingInFile{{Path: "/legacy.log", Multiline: &bad}}}

		if err := validateConfig(config); err == nil {
			t.Error("validateConfig skipped the top level files list")
		}
	})
}

// --- Line counts and flush semantics ---------------------------------------

func TestAggregatorReportsLineCount(t *testing.T) {
	a := mustAggregator(t, FlingMultiline{
		StartPattern: `^\d{4}-`,
		Mode:         multilineModeHaltBefore,
		TimeoutMS:    1000,
	})

	events := feedAndFlush(t, a, []string{
		// Arrives with no record open, so it is emitted alone. A non-matching line
		// only orphans in this position; once a record is open it joins that record.
		"orphan before any start line",
		"2026-07-28 record one",
		"  continuation",
		"  continuation",
		"2026-07-28 record two",
	})

	// orphan (1 line), record one (3), record two (1)
	wantCounts := []int{1, 3, 1}
	if len(events) != len(wantCounts) {
		t.Fatalf("got %d events, want %d\ngot: %q", len(events), len(wantCounts), eventTexts(events))
	}

	for i, want := range wantCounts {
		if events[i].Lines != want {
			t.Errorf("event %d Lines = %d, want %d (%q)", i, events[i].Lines, want, events[i].Text)
		}
	}
}

func TestFlushVersusFlushPartial(t *testing.T) {
	config := FlingMultiline{
		StartPattern: `^\d{4}-`,
		Mode:         multilineModeHaltBefore,
		TimeoutMS:    1000,
	}

	// Going idle is how a halt_before record legitimately ends, so its flush must
	// NOT be flagged - otherwise the last record of every quiet file looks
	// truncated. Which modes an idle flush does flag lives in
	// TestAggregatorIdleFlushFlagsModesThatWereCutShort.
	idle := mustAggregator(t, config)
	idle.add("2026-07-28 complete record", baseTime)

	event, ok := idle.idleFlush(baseTime.Add(time.Second))
	if !ok {
		t.Fatal("idleFlush found nothing buffered")
	}
	if event.Truncated {
		t.Error("idle flush marked the record truncated; a normally-ended record would look severed")
	}

	// A teardown flush cuts the record short, so it must be flagged.
	torn := mustAggregator(t, config)
	torn.add("2026-07-28 severed record", baseTime)

	event, ok = torn.flushPartial()
	if !ok {
		t.Fatal("flushPartial found nothing buffered")
	}
	if !event.Truncated {
		t.Error("teardown flush not marked truncated; a severed record would look complete")
	}

	// Neither should report anything on an empty buffer.
	empty := mustAggregator(t, config)
	if _, ok := empty.flush(); ok {
		t.Error("flush returned an event on an empty buffer")
	}
	if _, ok := empty.flushPartial(); ok {
		t.Error("flushPartial returned an event on an empty buffer")
	}
}

// --- log.flags merging -----------------------------------------------------

func TestAppendLogFlag(t *testing.T) {
	tests := []struct {
		name     string
		existing interface{}
		want     []string
	}{
		{name: "absent", existing: nil, want: []string{"multiline_truncated"}},
		{
			name:     "app emitted array (as decoded from JSON)",
			existing: []interface{}{"app_flag", "another"},
			want:     []string{"app_flag", "another", "multiline_truncated"},
		},
		{
			name:     "already a string slice",
			existing: []string{"app_flag"},
			want:     []string{"app_flag", "multiline_truncated"},
		},
		{name: "scalar string", existing: "app_flag", want: []string{"app_flag", "multiline_truncated"}},
		{name: "unusable type is discarded", existing: 42, want: []string{"multiline_truncated"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := appendLogFlag(test.existing, multilineTruncatedFlag)

			if len(got) != len(test.want) {
				t.Fatalf("got %q, want %q", got, test.want)
			}
			for i := range test.want {
				if got[i] != test.want[i] {
					t.Errorf("got %q, want %q", got, test.want)
					break
				}
			}
		})
	}
}

func TestSetLogFlag(t *testing.T) {
	// A plain-text event has no "log" object, so the flag goes on the flat dotted
	// key alongside fling's other fields.
	flat := map[string]interface{}{"message": "text"}
	setLogFlag(flat, multilineTruncatedFlag)

	flags, ok := flat["log.flags"].([]string)
	if !ok || len(flags) != 1 || flags[0] != multilineTruncatedFlag {
		t.Errorf(`flat event log.flags = %#v, want ["%s"]`, flat["log.flags"], multilineTruncatedFlag)
	}

	// An is_json source emitting nested ECS already has a "log" object. Writing the
	// dotted key here would put log.flags in the document twice.
	nested := map[string]interface{}{
		"log": map[string]interface{}{"level": "error", "flags": []interface{}{"app_flag"}},
	}
	setLogFlag(nested, multilineTruncatedFlag)

	if _, duplicated := nested["log.flags"]; duplicated {
		t.Error("wrote log.flags next to an existing log object; the document carries the field twice")
	}

	logObject := nested["log"].(map[string]interface{})
	nestedFlags, ok := logObject["flags"].([]string)
	if !ok {
		t.Fatalf("nested log.flags = %#v, want a string slice", logObject["flags"])
	}
	if len(nestedFlags) != 2 || nestedFlags[0] != "app_flag" || nestedFlags[1] != multilineTruncatedFlag {
		t.Errorf(`nested log.flags = %#v, want ["app_flag" "%s"]`, nestedFlags, multilineTruncatedFlag)
	}
	if logObject["level"] != "error" {
		t.Error("clobbered a sibling field inside the log object")
	}

	// The two multiline flags accumulate rather than replacing one another.
	both := map[string]interface{}{"message": "text"}
	setLogFlag(both, multilineTruncatedFlag)
	setLogFlag(both, multilineOversizedFlag)

	if flags, _ := both["log.flags"].([]string); len(flags) != 2 ||
		flags[0] != multilineTruncatedFlag || flags[1] != multilineOversizedFlag {
		t.Errorf("log.flags = %#v, want both multiline flags", both["log.flags"])
	}
}

// --- Timestamps ------------------------------------------------------------

func TestTimestampFor(t *testing.T) {
	got := timestampFor(baseTime)
	if want := "2026-07-28T10:23:45Z"; got != want {
		t.Errorf("timestampFor(%v) = %q, want %q", baseTime, got, want)
	}

	// A zero line time falls back to now rather than emitting a year-1 timestamp.
	if got := timestampFor(time.Time{}); strings.HasPrefix(got, "0001-01-01") {
		t.Errorf("timestampFor(zero) = %q, want a current timestamp", got)
	}
}

// captureLogs redirects logrus into a buffer for the duration of the test so an
// assertion can check what was reported.
func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()

	var buf bytes.Buffer
	previous := log.StandardLogger().Out
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(previous) })

	return &buf
}

// A file may name an output that no output block defines - a typo in the config
// is enough. dispatchEntry used to assert the map value straight to a channel,
// so the first event down that path took the whole process out. It must report
// the bad name and keep going instead.
//
// Any of these subtests panicking IS the regression: that is exactly what the
// guard exists to prevent.
func TestDispatchEntryUnknownOutput(t *testing.T) {
	event := FlingEvent{JSON: map[string]interface{}{"message": "hello"}}

	t.Run("delivers to a known output", func(t *testing.T) {
		channel := make(chan FlingEvent, 1)
		dispatchEntry(event, []string{"good"}, map[string]interface{}{"good": channel})

		select {
		case got := <-channel:
			if got.JSON["message"] != "hello" {
				t.Errorf("delivered %v, want the dispatched event", got.JSON)
			}
		default:
			t.Fatal("known output received nothing")
		}
	})

	t.Run("drops an undefined output and says which one", func(t *testing.T) {
		logs := captureLogs(t)

		dispatchEntry(event, []string{"typo-output"}, map[string]interface{}{})

		if !strings.Contains(logs.String(), "typo-output") {
			t.Errorf("error did not name the offending output, got: %s", logs.String())
		}
		if !strings.Contains(logs.String(), "unknown output") {
			t.Errorf("error did not explain the cause, got: %s", logs.String())
		}
	})

	t.Run("a bad output does not stop the ones after it", func(t *testing.T) {
		captureLogs(t)

		channel := make(chan FlingEvent, 1)
		// The bad name comes first: a guard that returned instead of continuing
		// would silently cost this file its working output too.
		dispatchEntry(event, []string{"typo-output", "good"}, map[string]interface{}{"good": channel})

		select {
		case <-channel:
		default:
			t.Fatal("the output listed after an undefined one received nothing")
		}
	})

	t.Run("drops an output that is not a channel", func(t *testing.T) {
		logs := captureLogs(t)

		// Same type assertion, different failure: the key exists but holds
		// something that cannot be published to.
		dispatchEntry(event, []string{"wrong-type"}, map[string]interface{}{"wrong-type": "not a channel"})

		if !strings.Contains(logs.String(), "wrong-type") {
			t.Errorf("error did not name the offending output, got: %s", logs.String())
		}
	})

	t.Run("an empty output list is a no-op", func(t *testing.T) {
		dispatchEntry(event, nil, map[string]interface{}{})
	})
}
