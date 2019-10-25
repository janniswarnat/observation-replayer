# OGC Observation Replayer

Part of the MONICA Toolbox to replay OGC Observations stored by GOST servers (https://github.com/gost/server) in Postgres databases during pilot events for testing and demonstration purposes. The Observations will be replayed in ascending order by their Observation id´s.

## Prerequisites

* A Postgres database filled by a GOST server instance (e.g. during a MONICA pilot event).
* An MQTT broker to publish the replayed Observations to.
* A GOST target instance configured to subscribe to the replayed Observations via the MQTT broker. All OGC "entities" (Observations, Datastreams, Things, Sensors etc.) need to be set up in the GOST target instance in advance to make sense of the incoming Observations.

## Run

Copy the example file for docker-compose, rename it to `docker-compose.yml` and define the environment variables according to your use case:

* `BROKER_URI`: Broker to publish the replayed Observations to, e.g. `tcp://<hostname>:1883` or `ssl://<hostname>:8883`
* `BROKER_USERNAME`: User to access the broker if necessary (optional)
* `BROKER_PASSWORD`: Password to access the broker if necessary (optional)
* `GOST_MQTT_PREFIX`: Replayed Observations will be replayed to `<GOST_MQTT_PREFIX>/Datastreams(<stream_id>)/Observations`. Make sure your GOST server subscribing to the replayed Observations is configured accordingly.
* `GOST_DB_HOST`: The hostname of your Postgres server
* `GOST_DB_DATABASE`: The name of your GOST database to replay the Observations from
* `GOST_DB_USER`: Postgres user with permissions to select from the GOST database
* `GOST_DB_PASSWORD`: Postgres password
* `DATASTREAM_IDS`: Comma separated list of Datastream id´s (optional). If not configured, all Datastreams will be replayed.
* `OBSERVATION_ID_START`: Observation id to start the replay from (optional, default is 0)
* `OBSERVATION_ID_END`: Id of the last Observation to be replayed (optional, default is `Integer.MAX_VALUE`)
* `OBSERVATION_INTERVAL_MS`: Interval between replayed Observations in milliseconds (optional, default is 1000)

Then start the replayer via 

```bash
docker-compose up
```

## Contribute

Feel free to create an issue or fork and create a pull request in GitHub in case you want to contribute to the tool.


