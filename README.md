# Realtime Data Streaming 

## Table of Contents
- [Introduction](#introduction)
- [System Architecture](#system-architecture)
- [Technologies](#technologies)
- [Getting Started](#getting-started)

## Introduction

This project is about building an near real-time data pipeline. This project is focused on developing an application that can perform real-time analysis of the weather conditions
## System Architecture

![System Architecture](images/system_architecture.jpg)

The project is designed with the following components:

- **Data Source**: I use source: Weather API from [weather API](https://www.weatherapi.com/)
- **Apache Airflow**: Responsible for orchestrating the pipeline and storing fetched data in a PostgreSQL database.
- **PostgreSQL**: Store metadata of Airflow.
- **Apache Kafka**: Used for streaming data from Cassandra to the processing engine.
- **Apache Spark**: For data processing with its master and worker nodes.
- **Cassandra**:  Where the processed data will be stored
- **Docker**: Used to containerize the services.
- **Grafana**: For visualization of the data.


## Technologies

- Apache Airflow
- Python
- Apache Kafka
- Apache Zookeeper
- Apache Spark
- Cassandra
- PostgreSQL
- Docker


## Getting Started

### Start pipeline and dashboard

1. Run Docker Compose to spin up the services:
    ```bash
    docker-compose up -d
    ```

2. Access airflow webserver UI (http://localhost:8080/) to start the job 


3. Run spark-job 
  - `<spark-master container id>`: get it in docker
  - `<spark master IP address>`: get it on the spark UI

    ```bash
      docker exec -it <spark-master container id>\
      spark-submit --master spark://<spark master IP address>:7077 \
      --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,\
      org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 spark_stream.py
    ```

    or if already have spark in machine use:

    ```bash
    python spark_stream.py
    ```

