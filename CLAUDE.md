# Working in this repo

fling tails log files and ships events to Pub/Sub, BigQuery, Elasticsearch or a
logger. One Go module, one package: `main.go` plus its tests at the repo root.

## Formatting

**Run `gofmt -w .` before every commit. `gofmt -l .` must print nothing.**

This is not a style preference — unformatted code produces diff noise that buries
the real change, and it drifts silently because nothing fails the build over it.
If you touch a file, it leaves your hands formatted.

Do not fold a formatting sweep into a behaviour change. If `gofmt -l .` reports
files you did not otherwise modify, format them in their own commit so the
substantive diff stays readable.

## Tests

`go test ./...` is the entire pipeline: unit tests plus end-to-end scenarios that
run a compiled fling against a real Pub/Sub emulator.

**The emulator tests never skip.** Pub/Sub is the point of this tool, so a green
run that quietly tested nothing is worse than a red one. Missing prerequisites
fail the test naming the command that fixes them. Do not add a build tag, a
`testing.Short()` gate, or a `t.Skip()` to make them optional.

They need a JRE and the gcloud emulator component:

```
brew install openjdk@21
export PATH="/opt/homebrew/opt/openjdk@21/bin:$PATH"
gcloud components install pubsub-emulator
```

`/usr/bin/java` on macOS resolves even with no JDK installed, so check
`java -version` runs, not just that `java` is on PATH.

### Writing tests here

- `main_test.go` — white-box unit tests, `package main`, reaches internals.
- `integration_test.go` — end-to-end, drives the compiled binary and asserts on
  what landed on the subscription.

Two rules the integration suite depends on:

1. **Every wait is bounded, and each bound is an assertion.** No fixed `sleep`
   waiting for the system to catch up. Poll until the condition holds or the
   deadline expires, and on expiry fail with what you wanted versus what you got.
   Prefer signals the process actually emits over elapsed time.

2. **Fixture setup happens before fling starts; appends happen after.** That
   split is what the `read_from_beginning` and tail-mode scenarios exist to test.
   Scenario 1's `ForbiddenLines` is what proves a tail-mode source really skipped
   pre-existing content, and it only means something if the file was populated
   before startup.

## Dependencies

One `go.mod`. A second module for test tooling was removed deliberately: it
duplicated the same transitive tree, so every advisory arrived twice and the test
harness could silently drift to a different Pub/Sub client version than the
binary it was verifying.

Check `govulncheck ./...` after dependency changes.

## Do not commit

The compiled `fling` binary. It is gitignored; build it with `go build .`.
