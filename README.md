# Fling

Fling is a Go log shipper and rotator focused on getting logs into Google Cloud Pub/Sub.

It was built for environments where applications running in containers cannot or prefer not to use stdout logging. Without log rotation, these container-local log files grow until Kubernetes nodes run out of disk — Fling solves both problems by shipping log lines to Pub/Sub and rotating the files on a configurable interval.

## Installation

Download a binary from [GitHub Releases](https://github.com/ptinsley/fling/releases), or install from source:

```bash
go install github.com/ptinsley/fling@latest
```

## Authentication

Fling supports two methods for authenticating to Google Cloud Pub/Sub:

1. **Default credentials** (recommended) — Fling will use [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials) automatically. This includes `GOOGLE_APPLICATION_CREDENTIALS` environment variable, GKE Workload Identity, or credentials configured via `gcloud auth application-default login`. To use default credentials, omit the `auth_file` field from your output configuration.

2. **Explicit service account file** — Set `auth_file` on the Pub/Sub output to the path of a service account JSON key file.

## Usage

```
fling -c <config.json> [-d] [-i] [--version]
```

| Flag | Description |
|------|-------------|
| `-c`, `--config` | **(required)** Path to JSON configuration file |
| `-d`, `--debug` | Enable debug logging |
| `-i`, `--inotify` | Use inotify for file monitoring (instead of polling) |
| `--version` | Print version and exit |

## Configuration Reference

Fling is configured with a JSON file. The top-level structure has three sections: `input`, `rotations`, and `output`.

### input.files[]

| Field | Type | Description |
|-------|------|-------------|
| `path` | string | File path to tail (or glob pattern if `is_glob` is true) |
| `is_json` | bool | Parse each line as JSON instead of wrapping in a `message` field |
| `is_glob` | bool | Treat `path` as a glob pattern and watch for new matching files |
| `glob_interval` | int | Seconds between glob re-evaluation (default: 30) |
| `read_from_beginning` | bool | Start reading from the beginning of the file instead of the end (default: false) |
| `outputs` | []string | List of output names to send lines to |
| `injections` | []object | Fields to inject into every log line (see below) |
| `multiline` | object | Join records that span several lines into one event (see [Multi-line Records](#multi-line-records)). Omit for one-event-per-line. |

### injections[]

Each injection adds a field to the log entry. Exactly one of `value`, `env_value`, or `hostname` should be set.

| Field | Type | Description |
|-------|------|-------------|
| `field` | string | Key name to inject into the log entry |
| `value` | string | Static string value |
| `env_value` | string | Name of an environment variable whose value will be injected |
| `hostname` | bool | If true, inject the system hostname |

### multiline

By default each physical line becomes one event. That shreds any record spanning
several lines — a MySQL crash backtrace, an InnoDB monitor dump, an nginx `error.log`
entry carrying application stderr — into unrelated documents downstream.

Set `multiline` on an input file to assemble those records before they are published.
Lines are joined with `\n` and the event is timestamped from its **first** line.

| Field | Type | Description |
|-------|------|-------------|
| `start_pattern` | string | **(required)** Regex a line must match to open a new record |
| `condition_pattern` | string | Regex used with `mode` to decide whether the record continues. Defaults to `start_pattern` for `halt_before` only; **required** for every other mode |
| `mode` | string | **(required)** One of the four modes below |
| `timeout_ms` | int | **(required)** Emit a record after this long without a new line. Without it, the last record of an idle file would never ship |
| `max_lines` | int | Force-emit after this many lines (default: 500) |
| `max_bytes` | int | Force-emit at this size (default: 262144) |

Modes match [Vector's file source](https://vector.dev/docs/reference/configuration/sources/file/),
so a pattern can move between the two without reinterpretation:

| Mode | Meaning | Typical use |
|------|---------|-------------|
| `halt_before` | Record ends *before* the next line matching `condition_pattern` | Timestamp-prefixed logs |
| `halt_with` | Record ends *with* the first matching line, inclusive | Explicitly terminated records |
| `continue_through` | Consecutive lines matching `condition_pattern` belong to the record | Indented stack traces |
| `continue_past` | A matching line means the *next* line belongs to the record | Trailing-backslash continuations |

`condition_pattern` is only defaulted for `halt_before`, because that is the one mode where
"a record runs until the next line that looks like a start line" is meaningful. Defaulting it
elsewhere would produce a silent no-op — `halt_with` would close every record on the line
that opened it, and the `continue_*` modes match continuation lines, which never look like a
start line.

Four guarantees worth relying on:

- **No line is ever dropped.** A line arriving with no open record and no `start_pattern`
  match — which happens routinely, since fling seeks to the end of a file and so starts
  mid-record — is emitted on its own. A pattern that matches nothing therefore degrades to
  exactly the old one-event-per-line behaviour rather than losing data.
- **A cut-short record is always marked.** Hitting `max_lines`/`max_bytes`, or having the
  tail torn down mid-record, adds `"log.flags": ["multiline_truncated"]`. Once a limit has
  cut a record, the lines that follow are its tail rather than records of their own, so they
  keep being assembled — into further chunks that stay flagged — until that record ends. It
  ends four ways, and all four end the run of flagged chunks: its own delimiter arrives
  (`halt_with`, `continue_past`), a `start_pattern` line opens a genuine new record, a
  `condition_pattern` boundary line does the same under `halt_before`, or the file goes quiet.
  An idle-timeout flush is *not* marked under `halt_before` or `continue_through`, where
  running out of continuation lines is how a record legitimately ends, but it *is* marked
  under `halt_with` (the terminator never arrived) and `continue_past` (the line the last
  marker promised never arrived).
- **A record that is merely too big is marked differently.** A single physical line that meets
  `max_bytes` on its own arrives whole — nothing was cut from it — so it carries
  `"log.flags": ["multiline_oversized_line"]` instead of `multiline_truncated`. Anything
  stitched onto it afterwards *was* cut, and is flagged `multiline_truncated` as usual. The two
  are independent rather than exclusive: a lone oversized line that is itself the tail of a
  record an earlier limit cut carries both, because both are true of it. Alert on
  `multiline_truncated` when you care about losing record content, and on
  `multiline_oversized_line` when you care about event size.
- **Every assembled event reports its size.** `fling.multiline.line_count` is the number of
  physical lines joined. This is the field to check when tuning a pattern: every record at
  `1` means the pattern never matched, records pinned at `max_lines` mean it is swallowing
  unrelated lines.

Bad patterns fail at startup, not per line: every regex is compiled when the config loads.

`max_bytes` is advisory — it counts the joined record exactly, but the limit is only checked
after a line is appended, so a record can overshoot by the line that crossed it. A single line
longer than `max_bytes` is never split: it ships whole, flagged `multiline_oversized_line`.
`max_bytes` exists to bound a runaway *record* well below Pub/Sub's 10 MB message limit, not to
size events precisely, and it cannot bound one enormous line.

**Size it against whatever consumes the topic, not against Pub/Sub.** If your consumer
enforces its own ceiling, `max_bytes` must sit below it with headroom, because that ceiling
almost certainly applies to the whole serialised event rather than to `message` alone:
injected fields cost a few hundred bytes, JSON escaping inflates the message (every joined
newline becomes `\n`, plus quote and backslash escaping), and the overshoot above adds a line.
Truncating here is better than being rejected downstream — a truncated record still arrives
carrying `log.flags`, whereas a rejected one is usually just gone.

#### MySQL error log

> **On MySQL 8, prefer the JSON error log for routed events.** Install
> `component_log_sink_json` and set `log_error_services='log_filter_internal; log_sink_json'`;
> fling can then tail `<log_error>.00.json` with `is_json: true` and no `multiline` at all.
>
> You still want the pattern below on the plain `log_error` file, because the fatal signal
> handler writes crash output (`mysqld got signal`, `Thread pointer:`, `Attempting backtrace`
> and the stack frames) directly to stderr with async-signal-safe calls — it never passes
> through the log sinks, so it is never in the JSON file.

```json
{
    "path": "/var/log/mysql/error.log",
    "outputs": ["k8s2elk"],
    "multiline": {
        "start_pattern": "^\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}:\\d{2}",
        "mode": "halt_before",
        "timeout_ms": 2000
    }
}
```

`[T ]` matches both the MySQL 8.0 form (`2026-07-28T10:23:45.123456Z`) and the older
space-separated form, and tolerates a `log_timestamps=SYSTEM` offset.

Raise `max_lines` / `max_bytes` above the defaults on this source if you care about InnoDB
monitor dumps — they routinely exceed 256 KiB. Nothing is dropped at the cap, but the dump
arrives as several `multiline_truncated` chunks instead of one document, so the transaction
and deadlock detail at the end has to be stitched back together by `@timestamp` and
`fling.source`.

#### MySQL slow query log

Every record here spans several lines, so without assembly the log is unusable.

```json
{
    "path": "/var/log/mysql/slow.log",
    "outputs": ["k8s2elk"],
    "multiline": {
        "start_pattern": "^# User@Host:",
        "mode": "halt_before",
        "timeout_ms": 2000
    }
}
```

**Anchor on `# User@Host:`, not `# Time:`.** MySQL writes `# Time:` only when the timestamp
changed since the previous entry, so queries landing in the same second omit it — a
`^# Time:` anchor silently merges those queries into the record above. Anchoring on
`# User@Host:`, which every entry has, is what Filebeat's mysql module does too.

The trade-off is that the `# Time:` belonging to the *next* entry attaches to the end of the
current record. Each query still keeps its own `# User@Host`, `# Query_time` and SQL together,
which is what matters for parsing. The header lines MySQL writes at startup (`Tcp port:`,
`Time Id Command Argument`) precede any `# User@Host:` and so ship as individual lines.

#### nginx error log

```json
{
    "path": "/webapp/log/nginx/error.log",
    "outputs": ["k8s2elk"],
    "multiline": {
        "start_pattern": "^(\\d{4}/\\d{2}/\\d{2} \\d{2}:\\d{2}:\\d{2} |\\[ [EWIDC] \\d{4}-\\d{2}-\\d{2} )",
        "mode": "halt_before",
        "timeout_ms": 1000
    }
}
```

Matches nginx's own `2026/07/28 10:23:45 [error] …` prefix and Passenger's
`[ E 2026-07-28 … ]` prefix; untimestamped stderr and backtrace lines attach to the
entry above them. Sample real `error.log` content before rolling a pattern out widely —
what lands in that file varies by application.

#### Known limitations

- **No flush on shutdown.** `main` has no signal handling (see the Todo at the top of
  `main.go`), so on SIGTERM anything still assembling is lost. The window is bounded by
  `timeout_ms`, but it overlaps the case you most want captured: a crash record is often
  mid-assembly exactly when the process is being killed. Keep `timeout_ms` low on sources
  where this matters. Fixing it properly needs a shutdown path that also drains the output
  channels, which fling does not have yet.
- **`is_json` with `multiline`** is allowed — the record is assembled first, then parsed as
  JSON, which covers pretty-printed JSON. But an unparseable result is dropped rather than
  degraded, so a mismatched pattern on a JSON source loses the whole record instead of one
  line. Prefer `is_json` alone for newline-delimited JSON. When the parsed record already
  carries a nested `log` object, the truncation flag is added inside it rather than as a flat
  `log.flags` key, so the document never carries the same field twice.
- **Wide globs are unbounded.** Memory is roughly `max_bytes` × number of tailed files. That
  is fine for a specific file, but putting `multiline` on an open-ended glob has no ceiling.
- **A file that never falls quiet is never recycled.** fling rebuilds each tail hourly to
  release handles on rotated files. On a `multiline` input the teardown additionally waits for
  a 5 s lull, because the rebuilt tail seeks to the end and a line written across the teardown
  would be lost. A file busy enough never to be quiet for 5 s therefore holds its handles for
  the life of the process. Inputs without `multiline` fare no better — their recycle needs a
  full hour of silence — so this is not specific to assembly, but it does mean the hourly
  recycle is best-effort rather than a bound.

#### Ruby / Java stack traces on a plain-text app log

```json
{
    "multiline": {
        "start_pattern": "^\\d{4}-\\d{2}-\\d{2}",
        "condition_pattern": "^(\\s|from\\s)",
        "mode": "continue_through",
        "timeout_ms": 1000
    }
}
```

### rotations[]

| Field | Type | Description |
|-------|------|-------------|
| `files` | []string | Glob patterns of files to rotate |
| `rotate_command` | string | Shell command to run after files are renamed (see [Log Rotation](#log-rotation)) |
| `rotate_interval` | int | Base interval in seconds between rotations |

### output.pubsub[]

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Identifier referenced by `outputs` in input files |
| `project` | string | Google Cloud project ID |
| `topic` | string | Pub/Sub topic name |
| `auth_file` | string | Path to service account JSON key (omit for default credentials) |

### Environment Variables

| Variable | Description |
|----------|-------------|
| `ROTATE_WINDOW` | Maximum random offset in seconds added to `rotate_interval` (default: 600). See [Log Rotation](#log-rotation). |

## Log Rotation

Fling can rotate log files on a schedule. Each rotation cycle:

1. Renames matching files to `<filename>.old`.
2. Executes `rotate_command` via a shell (`sh -c`).

**Why `rotate_command` exists:** After Fling renames a log file, the application that writes to that file still has the old file descriptor open and will continue writing to the renamed `.old` file. The `rotate_command` is used to signal the application to reopen its log files — typically by sending a SIGHUP or using a service manager command (e.g., `s6-svc -h /var/run/s6/services/nginx`).

**Thundering herd prevention:** When many Fling instances rotate simultaneously, the downstream applications may all restart at once. To avoid this, Fling adds a random offset (0 to `ROTATE_WINDOW` seconds, default 600) to each `rotate_interval`. Set `ROTATE_WINDOW` to tune the spread, or set it to a small value (e.g., `1`) to effectively disable randomization.

## Configuration Example

```json
{
    "input": {
        "files": [
            {
                "path": "/webapp/log/production.log",
                "outputs": [
                    "k8s2elk"
                ],
                "injections": [
                    {
                        "field": "hostname",
                        "hostname": true
                    },
                    {
                        "field": "appname",
                        "env_value": "APPNAME"
                    },
                    {
                        "field": "generator",
                        "value": "rails"
                    }
                ]
            },
            {
                "path": "/webapp/log/nginx/access.jlog",
                "is_json": true,
                "outputs": [
                    "k8s2bq",
                    "k8s2elk"
                ],
                "injections": [
                    {
                        "field": "srchost",
                        "env_value": "HOSTNAME"
                    },
                    {
                        "field": "appname",
                        "env_value": "APPNAME"
                    },
                    {
                        "field": "generator",
                        "value": "nginx_access"
                    }
                ]
            }
        ]
    },
    "rotations": [
        {
            "files": [
                "/webapp/logs/nginx/access.jlog",
                "/webapp/logs/nginx/error.log"
            ],
            "rotate_command": "s6-svc -h /var/run/s6/services/nginx",
            "rotate_interval": 300
        }
    ],
    "output": {
        "pubsub": [
            {
                "name": "k8s2elk",
                "project": "project",
                "topic": "fling-k8s"
            },
            {
                "name": "k8s2bq",
                "project": "project",
                "topic": "fling-k8s-bqonly"
            }
        ]
    }
}
```

## Releasing

```bash
git tag -a vX.X.X -m 'about this version'
git push origin vX.X.X
goreleaser --rm-dist
```
