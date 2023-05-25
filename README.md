## Description

### kafka-data-generator

This repository contains a data generator script, `kafka-data-generator.py`, that generates data and puts it into a Kafka topic. The data generated can be subscribed to using PySpark(`writing_into_elastic.py`) and written into ElasticSearch for further analysis in Kibana.

### Prerequisites

Before running the `kafka-data-generator` script, ensure that you have the following:

- Kafka setup and running
- PySpark installed
- ElasticSearch and Kibana setup and running
- `kafka-data-generator` also require ENVIRONMENT_VARIABLE(KAFKA_HOST, TOPIC_NAME) which is default to "10.0.0.16:9092", "clickstream".
