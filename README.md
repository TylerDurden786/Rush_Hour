# Real-time Data Engineering Pipeline for Road Travel

## Table of Contents
1. [Introduction](#introduction)
2. [System Architecture](#system-architecture)
3. [Components](#components)
4. [What I Learnt](#what-i-learnt)
5. [Technologies](#technologies)
6. [Getting Started](#getting-started)

## Introduction
This project aims to create a real-time data streaming pipeline for road travel, specifically focusing on the Pune-Mumbai route. It collects various data sources from iot devices to provide insights and visualization for optimizing road travel, improving safety, and enhancing the overall travel experience.

## System Architecture
![process_flow](https://github.com/TylerDurden786/Rush_Hour/assets/168437985/f6757c8a-2ab9-463e-81c8-60962c299a60)

## Components
- Docker Compose: Manages the deployment and orchestration of the project's services.
- Apache Kafka: Provides a distributed streaming platform to handle high volume data streams efficiently.
- Apache Spark: Processes and analyzes large-scale data streams in real-time.
- Confluent Kafka Python Client: Facilitates interaction with Apache Kafka in Python applications.

## What I Learnt
1. Understanding and implementing real-time data streaming pipeline using Apache Kafka and Apache Spark.
2. Utilizing Docker Compose for containerization and orchestration of multi-service applications.
3. Using basics of AWS S3, Glue and Redshift.

## Technologies
- Apache Kafka
- Apache Spark
- Docker
- Python
- Confluent Kafka Python Client

## Getting Started
### 1. Clone the repository:
```bash
  git clone https://github.com/TylerDurden786/Rush_Hour.git
```
### Navigate to the project directory:
```bash
  cd rush_hour
```
### Run Docker Compose to spin up the services:
```bash
  docker-compose.yaml
```