# 📈 Servers Metrics & Logs Project

## Overview

This project simulates a cloud storage environment with 10 servers and a load balancer, collecting system metrics and logs. A **Kafka** cluster streams the data, with two dedicated topics.
A Python consumer stores **server metrics** into a **SQL Server database**, and a **Spark Structured Streaming** application processes **load balancer logs** to compute 5-minute operational summaries, saving results into **HDFS**.

---

## Architecture

![image](https://github.com/user-attachments/assets/084a51a4-50e5-4e3a-b624-275f44718241)


---

## Setup Instructions

### 1. Start Kafka Broker

Run Kafka inside Docker using **KRaft mode** (no Zookeeper).

Start the Kafka broker:

```bash
docker-compose up -d
```

---

### 2. Create Kafka Topics

```bash
# Create topics using kafka-topics CLI
kafka-topics.sh --create --topic test-topic3 --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

kafka-topics.sh --create --topic test-topic4 --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

* **test-topic3** → Logs from Load Balancer
* **test-topic4** → Metrics from Servers

---

### 3. Run Java Producer Simulator

```bash
# Download dependencies and run
mvn clean compile exec:java
```

This simulates the 10 server agents and the load balancer agent sending data to Kafka.

---

### 4. Start Python Metrics Consumer

**File:** `metrics_consumer.py`

* Consumes from `test-topic4`.
* Parses server metrics.
* Batch inserts every 2 minutes (or 1000 messages) into **MSSQL** table `server_matric`.

```bash
python metrics_consumer.py
```

**Requirements:**

* kafka-python
* pandas
* sqlalchemy
* pyodbc

### 📸 Output:
![image](https://github.com/user-attachments/assets/ab8b7988-f31d-47e9-9b61-f646e7eb476e)
![image](https://github.com/user-attachments/assets/9d0434fc-87e9-45e9-8131-d833a60608c4)

---

### 5. Start Spark Logs Processor

**File:** `log_consumer.py`

* Consumes from `test-topic3`.
* Parses HTTP logs (GET/POST success/failure).
* Computes 5-minute moving windows with 10-min watermark.
* Writes aggregated results to `output_log_summary.csv`.

```bash
python log_consumer.py
```

**Requirements:**

* PySpark
* findspark
* pandas
* os

### 📸 Output:
![image](https://github.com/user-attachments/assets/0dc53bda-e3c2-4fcf-b867-321dfcd034f2)

---

### 6. Upload Output to HDFS

**File:** `hdfs_uploader.py`

* Uploads the generated `output_log_summary.csv` file to **HDFS** every 5 minutes, overwriting the previous version.

```bash
python hdfs_uploader.py
```

**Requirements:**

* hdfs
* pandas

### 📸 Output:
![image](https://github.com/user-attachments/assets/edd97e28-cd7c-4efd-bd0b-b8d9d4e718ff)

---

## Technologies Used

* **Apache Kafka** (broker, topics, KRaft mode)
* **Maven** (Java producers)
* **Python** (kafka-python, sqlalchemy, pandas)
* **Apache Spark Structured Streaming** (PySpark)
* **Hadoop HDFS** (for final CSV storage)
* **SQL Server** (Microsoft SQL Server for storing metrics)

---

## Design Choices

* Single Kafka broker (lab simulation).
* Use of **batching** for efficient database inserts.
* **Watermarking** to handle late arriving logs in Spark.
* Use of **KRaft** (no Zookeeper) for simplicity.
* Separate consumers for metrics vs logs for scalability.

---

# 🚀 How to Run Everything in 5 Minutes

```bash
docker-compose up -d        # Start Kafka broker
mvn clean compile exec:java # Start Java producer simulation
python metrics_consumer.py  # Start metrics consumer
python log_consumer.py      # Start Spark logs consumer
python hdfs_uploader.py     # Start HDFS uploader
```

