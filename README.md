## Indexing Service

Current directory contains:
* indexing-service implementation in `src/esindex` folder

VERSION: `0.2`

### Docker container

Using the Indexing Service Docker container:
* `docker pull attxproject/index-service:dev` in the current folder;
* running the container `docker run -d -p 4304:4304 attxproject/index-service:dev` runs the container in detached mode on the `4304` port (production version should have this port hidden);
* using the endpoints `http://localhost:4304/{versionNb}/{endpoint}` or the other listed below.

The version number is specified in `src/esindex/app.py` under `version` variable.

## Overview

The Indexing service manages interaction with Elasticsearch 5.+ and retrieving statistics about it (e.g. list of existing aliases) but it main function is to index data in Elasticsearch.

Full information on how to run and work with the Indexing Service available at: https://attx-project.github.io/Service-Indexing.html

## API Endpoints

The Indexing REST API has the following endpoints:
* `alias` - retrieves information about existing aliases;
* `data` - index provided data into Elasticsearch;
* `health` - checks if the application is running.

## Develop

### Requirements
1. Python 2.7
2. Gradle 3.0+ https://gradle.org/
3. Pypi Ivy repository either a local one (see https://github.com/linkedin/pygradle/blob/master/docs/pivy-importer.md for more information) or you can deploy your own version using https://github.com/blankdots/ivy-pypi-repo

### Building the Artifact with Gradle

Install [gradle](https://gradle.org/install). The tasks available are listed below:

* do clean build: `gradle clean build`
* see tasks: `gradle tasks --all` and depenencies `gradle depenencies`
* see test coverage `gradle pytest coverage` it will generate a html report in `htmlcov`

### Run without Gradle

To run the server, please execute the following (preferably in a virtual environment):
```
pip install -r pinned.txt
python src/esindex/indexservice.py server
python src/esindex/indexservice.py rpc
```

For testing purposes the application requires a running Elasticsearch, RabbitMQ. Also the health endpoint provides information on running services the service has detected: `http://localhost:4304/health`
