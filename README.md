# 📊 Servers Metrics & Logs Monitoring System

## 🚀 Project Overview

We have a **cluster of 10 servers** hosting a cloud storage website where users can upload and store various types of files. In addition, there's a **load balancer** acting as the main gateway to the website.

Each server and the load balancer have **agents** deployed to collect:

* **Metrics** from the 10 servers (e.g., CPU, memory)
* **Logs** from the load balancer (e.g., HTTP operations)

This project involves designing a **multi-node Kafka cluster** and building a complete data pipeline to process and store the collected data.

---

## 🧩 Architecture

### ✅ Kafka Topics

* `server-metrics`: receives metrics from the 10 servers
* `loadbalancer-logs`: receives HTTP logs from the load balancer

### 📥 Data Producers

A Java program simulates the agents sending data to the Kafka topics.

Run it using Maven:

```bash
mvn exec:java
```

### 🧑‍💻 Data Consumers

* **Metrics Consumer**:

  * Language: Your choice (e.g., Python, Java)
  * Reads from `server-metrics`
  * Stores data in a **relational database** (e.g., PostgreSQL)

* **Logs Processor (Spark Application)**:

  * Consumes from `loadbalancer-logs`
  * Calculates **moving window counts** (5 minutes) for:

    * Successful GET
    * Successful POST
    * Failed GET
    * Failed POST
  * Stores the results into **HDFS**

---

## 🛠️ Requirements

* Deploy a **multi-node Kafka cluster**
* Create the following Kafka topics:

  * `server-metrics`
  * `loadbalancer-logs`
* Run the provided **Java producer**
* Implement:

  * Kafka consumer for metrics → relational DB
  * Spark application for logs → HDFS

---

## 📦 Key Deliverables

* 📝 Configuration file for each Kafka broker and topic
* 💻 Source code for:

  * Kafka metrics consumer
  * Spark logs processor
