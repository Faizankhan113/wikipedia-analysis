# 📊 Wikipedia Real-Time Data Pipeline (Kafka + Spark + Docker)

## 🚀 Overview

This project builds a **real-time data pipeline** that streams live Wikipedia changes, processes them using Apache Spark, and manages messaging with Apache Kafka — all containerized using Docker.

---

## 🏗️ Architecture

```
Wikipedia Stream → Kafka (Producer) → Spark Streaming → Output
```

* **Producer**: Fetches live Wikipedia edits
* **Kafka**: Handles real-time messaging
* **Spark**: Processes streaming data
* **Docker**: Runs everything in isolated containers

---

## 🛠️ Tech Stack

* 🐳 Docker & Docker Compose
* ⚡ Apache Kafka (KRaft mode)
* 🔥 Apache Spark (Structured Streaming)
* 🐍 Python

---

## 📂 Project Structure

```
.
├── docker-compose.yml
├── jobs/
│   ├── spark-streaming.py
│   ├── checkpoint/
│   └── output/
├── producer/
│   └── python_wikipedia.py
├── spark/
│   └── Dockerfile
├── runtime/
│   └── conf/
├── .gitignore
└── README.md
```

---

## ⚙️ Setup & Run

### 1. Clone the repository

```
git clone https://github.com/Faizankhan113/wikipedia-analysis.git
```
```
cd wikipedia-analysis
```

---

### 2. Start services 

#### First Time
```
docker compose up --build
```
#### Repeating runs
```
docker compose up -d
```

> **Note:** use below command to stop the running program.
> ```
> docker compose down
> ```

---

### 3. What happens

* Kafka starts and auto-creates topics
* Producer streams Wikipedia data
* Spark consumes and processes the stream
* Output is written to `jobs/output/`

---

## 📡 Kafka Topic

* `wiki-changes` → stores real-time Wikipedia edits

---

## 📈 Output

Processed data is saved in:

```
jobs/output/
```

Checkpointing (for fault tolerance):

```
jobs/checkpoint/
```

---

## 👨‍💻 Author

Faizan Khan

---

## ⭐ If you like this project

Give it a star on GitHub!

