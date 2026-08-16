# Developer Analytics Platform

A real-time analytics pipeline built on live GitHub activity

## Project structure

```
developer_analytics_platform/
├── backend/
│   ├── config/settings.py      # all connection strings / tunables, in one place
│   ├── producer/               # polls GitHub, publishes to Kafka
│   ├── streaming/              # Spark Structured Streaming job 
│   ├── storage/                # MongoDB access layer, indexes, sharding
│   └── service/                # query functions the dashboard calls, activity categories
├── frontend/dashboard/         # Streamlit app
└── infra/                      # Docker Compose: Kafka cluster + sharded MongoDB cluster
```

## Dashboard features

- **Event volume over time**: line chart, events/minute by event type
- **Activity category breakdown**: Code Activity / Discussion / Social-Attention, bucketed from event types
- **Trending repos**: repos with the sharpest *increase* in activity
- **Most active repos (overall)**: highest total event count
- **New repos**: repos first seen within the lookback window
- **Top contributors**: most active GitHub actors (usernames)
- **Top event types**: raw event-type totals

## Prerequisites

- Docker + Docker Compose
- Python 3.10+
- Java (required by PySpark) (check with `java -version`)
- 4GB free RAM for the Docker containers

## Setup

```bash
# 1. Install Python dependencies
python3 -m venv venv && source venv/bin/activate   # or your preferred env tool
pip install -r requirements.txt

# 2. Configure environment
nano .env    # defaults already match the Docker Compose setup

# 3. Start infrastructure
cd infra
docker-compose up -d
cd ..

# 4. Create the Kafka topic (run ONCE)
bash infra/init/init_kafka_topic.sh

# 5. Set up MongoDB cluster

# 6. Create MongoDB indexes
python -m backend.storage.indexes
```

## Running the pipeline

Open three terminals (all from the project root, with your virtualenv active):

**Terminal 1: Producer** (polls GitHub, publishes to Kafka)
```bash
python -m backend.producer.kafka_producer
```

**Terminal 2: Streaming job** (aggregations)
```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  backend/streaming/job.py
```

**Terminal 3: Dashboard**
```bash
streamlit run frontend/dashboard/app.py
```
Then open the URL Streamlit prints (usually http://localhost:8501).
