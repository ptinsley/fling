# fling
Fling is a golang logshipper / rotator focussed on getting logs into Google Cloud Pub/Sub. The main difference between this and other logshippers is that it is also capable of rotating logs.

We have several different log types that are generated in containers and either cannot or don't want to use STDOUT logging. This causes the obvious problem of steadily increasing disk space until kubernetes nodes run out and bad things happen.

# Cutting a new version of Fling
```
git tag -a vX.X.X -m 'about this version'
git push origin vX.X.X
goreleaser --rm-dist
```


Config File Example:
```
{
    "files": [
        {
            "path": "/webapp/log/production.log",
            "outputs": [
                "k8s2elk"
            ],
            "injections":[
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
            "path": "/webapp/log/passenger.8080.log",
            "outputs": [
                "k8s2elk"
            ],
            "injections":[
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
                    "value": "passenger"
                }
            ]
        },
        {
            "path": "/webapp/log/newrelic_agent.log",
            "outputs": [
                "k8s2elk"
            ],
            "injections":[
                {
                    "field": "hostname",
                    "hostname": true
                },
                {
                    "field": "generator",
                    "value": "newrelic"
                }
            ]
        },
        {
            "path": "/webapp/log/nginx/error.log",
            "outputs": [
                "k8s2elk"
            ],
            "injections":[
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
                    "value": "nginx_error"
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
            "injections":[
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
    ],
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
    "outputs": [
      {
          "name": "k8s2elk",
          "type": "pubsub",
          "pubsub_project": "project-name",
          "pubsub_auth_file": "/super/secret/key.json",
          "pubsub_topic": "fling-k8s"
      },
      {
        "name": "k8s2bq",
        "type": "pubsub",
        "pubsub_project": "project-name",
        "pubsub_auth_file": "/super/secret/key.json",
        "pubsub_topic": "fling-k8s-bqonly"
      }
    ]
}
```