# redisbus

Python library implementing a Redis message bus and related tools.

The goal of this library is to provide a simple, small library built 
on Redis that makes it easy to connect stateful and stateless software
components written in Python.

This library was used during the bring-up of a Unreal prototype game.

The general concepts involved were used to support developers running their own 
portions of the game services stack in a local on-prem Kubernetes environment.

Site can be a branch/developer name or a strongly keyed name to reference
clusters of services.

It is possible to implement worker architectures easily with this library and
connect those architectures to Python based web services or database stateful
services with a minimal amount of code.

I find that using Redis as a general backing cache presents many advantages to 
service clusters. You can layer a lot of common functionality together: service
discovery, document caching, and publish-subscribe notifications. In addition, you
get a variety of addressing modes for sending messages to other clients. Direct,
broadcast, multi-cast.

#### Terms

* Subscription

A Redis subscription in a pub-sub topology

* Queue

A queue, specifically of messages stored in Redis

* Worker

Stateful object with automatic RPC capability

* Client

A connection to Redis supporting the message bus API


#### Requirements

+ Python 3


#### Install

redisbus is published on pip and can be installed easily:

```bash
pip install redisbus
```

#### Usage

redisbus can be used as a library or a command line. If you want to explore the functionality of the library the easiest way is to play with the command line application.


##### Redis Bus CLI

```bash
usage: redisbus-cli [-h] [--call CALL] [--wait WAIT] [--data DATA]
                  [--jsondata JSONDATA] [--hostname HOSTNAME] [--port PORT]
                  [--db DB] [--worker WORKER_TYPE] [--worker_id WORKER_ID]
                  [--worker_interval WORKER_INTERVAL]
                  [--worker_path WORKER_PATH] [--multicast MULTICAST]
                  [--local LOCAL] [--site SITE] [--spawner SPAWNER]
                  [--verbose]

Generic command line interface to the redis bus.

optional arguments:
  -h, --help            show this help message and exit
  --call CALL           call (RPC) to execute
  --wait WAIT           wait time for RPC response
  --data DATA           data for command
  --jsondata JSONDATA   data for command (json formatted)
  --hostname HOSTNAME   redis hostname
  --port PORT           redis port
  --db DB               redis database
  --worker WORKER_TYPE  name of worker type to run, or to address for --call
                        messages
  --worker_id WORKER_ID
                        worker ID used to address direct calls
  --worker_interval WORKER_INTERVAL
                        interval to tick workers
  --worker_path WORKER_PATH
                        path for worker operations
  --multicast MULTICAST
                        pattern for multicasting to workers e.g.:
                        10.130.*/10.130.10.13:*
  --local LOCAL         only send to workers local workers (worker_id will
                        override)
  --site SITE           site name to use for workers
  --spawner SPAWNER     spawning worker ID when launched from spawn
  --verbose             Enable debug logging
``` 

#### Build / Distribution

Use a virtual environment!

```bash
virtualenv venv
```

Activate the virtual environment.

POSIX:

```bash
. venv/bin/activate
```

Windows:

```cmd
venv\Scripts\activate.bat
```

Build and install:

```bash
pip install wheel
python setup.py bdist_wheel
pip install dist/redisbus-*-py3-none-any.whl
```

#### Special Thanks

Ian Richardson @Tibrim
