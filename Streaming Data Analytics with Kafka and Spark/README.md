# Real-Time Clickstream Data Processing/Data Analytics with Kafka and Spark Streaming

## Overview

This project simulates the real-time collection and processing of clickstream data. The clickstream data is generated using the Faker Python library and pushed to a Kafka topic. Apache Spark Streaming is then used to process the incoming data, perform necessary cleaning and aggregation, and finally store the results in Parquet format for efficient querying.

The architecture consists of the following components:
   Kafka for message brokering
   Spark Streaming for real-time data processing
   Parquet format for storage of processed data

## Architecture

1. **Data Generation**: A Python function generates clickstream data using the `Faker` module.
2. **Data Ingestion**: The generated data is sent to a Kafka topic.
3. **Data Processing**: Spark Streaming subscribes to the Kafka topic, processes the data (cleaning and aggregating).
4. **Data Storage**: The processed data is stored in Parquet format.

## Prerequisites

- **Kafka**: Installed and set up locally.
- **Python**: Installed with `Faker` module.
- **Jupyter Notebook**: For running PySpark code.
- **Spark**: Installed locally.

## Setup

### Kafka Setup

1. Download and extract Kafka from the official website.

2. Start the Zookeeper service:
   ```sh
   .\bin\zookeeper-server-start.sh config\zookeeper.properties

3. Start the Kafka service:
   ```sh
   .\bin\windows\kafka-server-start.bat .\config\server.properties

4. Check for Available Topics
   ```sh
   .\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092

5. Create Topic
   ```sh
   kafka-topics.bat --create --bootstrap-server localhost:9092 --topic test

6. Start Producer
   ```sh
   kafka-console-producer.bat --broker-list localhost:9092 --topic test

7. Start Consumer
   ```sh
   kafka-console-consumer.bat --topic test --bootstrap-server localhost:9092 --from-beginning

8. Stop Servers
   ```sh
   .\bin\windows\zookeeper-server-stop.bat .\config\zookeeper.properties

   
   .\bin\windows\kafka-server-stop.bat .\config\server.properties


## Running the Project
1. **Generate Data**: Create a Python script or Jupyter notebook that generates synthetic clickstream data using the Faker library and pushes it to Kafka.

2. **Consume Data**: Create another Python script or Jupyter notebook that listens to the Kafka topic using Spark Streaming and processes the data in real-time.

3. **Process and Store**:

   - Clean the data (e.g., handle missing values, timestamps, etc.).

   - Aggregate the data based on desired parameters (e.g., counts, user activity).

   - Write the output to Parquet format.

## To run the project:

   - Start your Kafka producer (which generates and sends data to Kafka).

   - Start the Spark Streaming job (which consumes and processes data).

   - Check your output folder to see the Parquet files with processed data.

## Data Flow
### The data flow of this project follows these steps:

1. **Kafka Producer**: Generates clickstream data using the Faker library and sends it to a Kafka topic (clickstream-topic).

2. **Kafka Consumer (Spark Streaming)**: Spark reads the stream from Kafka in real-time.

3. **Data Processing**: The data is cleaned (removal of null values, proper type casting, timestamp extraction) and aggregated (by user or event type).

4. **Storage**: The processed data is stored in Parquet format for further analysis.


# Contributing

If you'd like to contribute to this project, feel free to open an issue or submit a pull request. Any feedback, suggestions, or improvements are welcome!

## Steps to contribute:
1. Fork the repository.
2. Create a new branch (git checkout -b feature-xyz).
3. Make your changes and commit them (git commit -am 'Add feature XYZ').
4. Push to the branch (git push origin feature-xyz).
5. Open a pull request.
