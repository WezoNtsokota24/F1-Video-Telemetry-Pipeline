# 🏎️ F1 Video Telemetry Pipeline

**Real-time Computer Vision & Data Engineering pipeline to analyze Formula 1 Pit Stops.**

![Python](https://img.shields.io/badge/Python-3.9%2B-blue)
![Spark](https://img.shields.io/badge/Apache%20Spark-Streaming-orange)
![Kafka](https://img.shields.io/badge/Redpanda-Kafka-red)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED)

---

## 📖 Overview

This project builds an end-to-end data pipeline that processes raw video footage of F1 races to detect pit stops and calculate their duration. It uses **YOLOv8** for object detection, streams telemetry via **Redpanda (Kafka)**, processes the data using **Apache Spark Structured Streaming**, and stores the final analytics in a **MinIO Data Lake** (S3).



## 🏗️ Architecture

The pipeline consists of three main stages: **The Eyes (Ingestion)**, **The Spine (Messaging)**, and **The Brain (Processing)**.

```mermaid
graph LR
    A[🎥 F1 Video Source] -->|OpenCV Frame| B(🐍 Vision Producer)
    B -->|YOLOv8 Detection| C{Is Car in Pit Box?}
    C -->|Yes: PIT_ENTRY| D[🐼 Redpanda / Kafka]
    D -->|Stream Events| E[⚡ Apache Spark Engine]
    E -->|Filter & Transform| F[(💾 MinIO / S3 Data Lake)]
🛠️ Tech Stack
Language: Python 3.9+

Computer Vision: OpenCV, Ultralytics YOLOv8

Streaming/Messaging: Redpanda (Kafka compatible)

Processing Engine: Apache Spark (PySpark Structured Streaming)

Storage: MinIO (S3 Object Storage) with Parquet format

Infrastructure: Docker & Docker Compose

🚀 Getting Started
1. Prerequisites
Docker Desktop (must be running)

Python 3 installed on your local machine

2. Installation
Step 1: Clone the repository


git clone [https://github.com/YOUR_USERNAME/F1-Video-Telemetry-Pipeline.git](https://github.com/YOUR_USERNAME/F1-Video-Telemetry-Pipeline.git)
cd F1-Video-Telemetry-Pipeline
Step 2: Start the Infrastructure We use Docker to spin up Redpanda (Kafka), MinIO (Storage), and the Spark Master.


docker compose up -d
Step 3: Setup Local Python Environment (The Eyes) This environment runs the video player and computer vision model on your local machine.


python3 -m venv .venv
source .venv/bin/activate
pip install opencv-python ultralytics confluent-kafka
Step 4: Setup Spark Environment (The Brain) Since Spark runs inside a container, we need to install the dependencies there once.


docker exec -it f1-spark pip install pyspark confluent-kafka s3fs
🏁 How to Run
You need two separate terminals to run the full pipeline.

Terminal 1: The Brain 🧠 (Spark Processor)
This script listens to the Kafka stream, processes the data, and saves it to the Data Lake.


# Log into the Spark Container
docker exec -it f1-spark /bin/bash

# Run the processor
python processor.py
You should see: ✅ Pipeline Active. Data is being saved to s3a://f1-data/pit_stops

Terminal 2: The Eyes 👀 (Vision Producer)
This script plays the video, detects the F1 car, and sends events to Kafka.

Bash
# Make sure your virtual env is active
source .venv/bin/activate

# Run the video producer
python3 vision_producer.py
📊 Results & Data Access
The processed data is stored in Parquet format (columnar storage optimized for big data).

To inspect the files:

Open MinIO Console: http://localhost:9001

User: admin | Pass: password

Navigate to Bucket: f1-data -> pit_stops

Example Data Schema
JSON
{
  "frame_id": 450,
  "timestamp": 1769123456.789,
  "event_type": "PIT_ENTRY",
  "pit_timer": 3.45,
  "detections": ["car", "mechanic"]
}
🔮 Future Improvements
[ ] Dashboard: Build a Power BI or Streamlit dashboard to visualize average pit stop times.

[ ] Cloud Deployment: Migrate MinIO to real AWS S3 and run Spark on Databricks.

[ ] Team Recognition: Retrain YOLO to identify specific teams (Red Bull vs. Ferrari).
