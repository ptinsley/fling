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

### injections[]

Each injection adds a field to the log entry. Exactly one of `value`, `env_value`, or `hostname` should be set.

| Field | Type | Description |
|-------|------|-------------|
| `field` | string | Key name to inject into the log entry |
| `value` | string | Static string value |
| `env_value` | string | Name of an environment variable whose value will be injected |
| `hostname` | bool | If true, inject the system hostname |

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
goreleaser build --clean
```
