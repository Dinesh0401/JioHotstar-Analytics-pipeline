# 🎬 JioHotstar Media Streaming Analytics Platform

### Production-Scale Data Engineering, Analytics, AI Agent & MLOps Platform

*A complete end-to-end streaming analytics platform inspired by modern OTT companies such as **JioHotstar, Netflix, and Disney+**, built using **Apache Spark, Kafka, Delta Lake, Airflow, AWS, Machine Learning, and Generative AI.***

<img width="1693" height="929" alt="ChatGPT Image Jul 21, 2026, 03_20_06 PM" src="https://github.com/user-attachments/assets/26465dbf-455b-406f-ae85-8e926950cae5" />

---

## 📖 Overview

Modern streaming platforms generate millions of user interactions every day.

Every play, pause, search, rating, subscription purchase, and watch completion produces valuable business data.

This project demonstrates how an enterprise OTT platform collects, processes, analyzes, predicts, and explains these events using a modern **Lakehouse + AI Agent architecture**.

Unlike traditional ML projects that only train models, this project covers the **complete data lifecycle**:

* Data Engineering
* Real-time Streaming
* Medallion Lakehouse
* Analytics Engineering
* Machine Learning
* AI Reasoning
* Cloud Deployment
* Interactive Analytics

---

# 🎯 Business Problem

An OTT platform receives millions of events every day.

Examples include:

* User Login
* Content Playback
* Watch Duration
* Search Queries
* Ratings & Reviews
* Subscription Purchase
* Plan Renewal
* Campaign Clicks
* Device Information

Business teams constantly ask questions such as:

* Which content is trending?
* Which subscription plan generates the highest revenue?
* Which users are likely to cancel?
* Which genres are becoming popular?
* What should be recommended next?
* Why has user engagement decreased?

Instead of manually querying multiple databases, an AI Agent automatically understands business questions, retrieves analytics from curated data, and generates explainable insights.

---

# 🏗 Architecture

```
Streaming Applications
        │
        ▼
   Kafka Event Streaming
        │
        ▼
 Spark Bronze Layer (Raw)
        │
        ▼
 Spark Silver Layer (Validated)
        │
        ▼
 Spark Gold Layer (Business Analytics)
        │
        ├──────────────┐
        ▼              ▼
 Machine Learning   Athena Analytics
        │              │
        └──────┬───────┘
               ▼
        AI Reasoning Runtime
               │
   ┌───────────┼────────────┐
   ▼           ▼            ▼
Streamlit   FastAPI API     CLI
```

---

# 🚀 Technology Stack

## Cloud

* AWS S3
* AWS Athena
* Amazon Bedrock
* IAM

---

## Data Engineering

* Apache Spark
* Delta Lake
* Apache Kafka
* Apache Airflow

---

## Machine Learning

* Scikit-Learn
* Random Forest
* Spark MLlib ALS Recommendation Engine

---

## AI

* LangGraph
* Amazon Bedrock
* ReAct Agent
* Tool Calling
* Rule Engine

---

## Backend

* Python
* FastAPI

---

## Storage

* Delta Lake
* Amazon S3
* PostgreSQL (metadata & runtime information)

---

## Visualization

* Streamlit
* Plotly

---

# 🔄 Project Workflow

## Phase 1 — Synthetic Data Generation

Generate realistic OTT datasets including:

* Users
* Content Catalog
* Ratings
* Subscription Plans
* Viewing Events
* Marketing Campaigns

These datasets simulate production-scale streaming traffic.

---

## Phase 2 — Real-Time Event Streaming

User activity is continuously streamed through Kafka.

Examples:

* Play
* Pause
* Resume
* Watch Complete
* Search
* Subscription Purchase

Kafka acts as the real-time event bus.

---

## Phase 3 — Bronze Layer

Spark Streaming consumes Kafka topics.

The Bronze layer stores:

* Raw events
* Original schema
* Immutable records

Purpose:

* Data replay
* Auditing
* Disaster recovery

---

## Phase 4 — Silver Layer

Spark transforms raw data into trusted datasets.

Operations include:

* Schema validation
* Null handling
* Duplicate removal
* Type conversion
* Data enrichment
* Business rule validation
* Quality checks

Result:

Clean, standardized data ready for analytics.

---

## Phase 5 — Gold Layer

Business-ready analytics tables are generated.

Examples include:

* Daily Active Users
* Watch Time Analytics
* Subscription Metrics
* Genre Popularity
* User Engagement
* Content Rating Summary

These optimized tables power dashboards, reports, ML models, and AI tools.

---

## Phase 6 — Machine Learning

Analytics tables are used to train predictive models.

### Customer Churn Prediction

Algorithm:

Random Forest

Predicts users likely to cancel subscriptions.

---

### Content Popularity Prediction

Algorithm:

Random Forest

Forecasts future trending content.

---

### Personalized Recommendation Engine

Algorithm:

ALS Collaborative Filtering (Spark MLlib)

Recommends personalized content based on user behavior.

Model predictions are written back into Gold tables.

---

## Phase 7 — Analytics Layer

Curated analytics are queried using:

* Amazon Athena
* PostgreSQL (metadata and runtime information)

This enables fast analytical queries without directly accessing raw datasets.

---

## Phase 8 — AI Reasoning Runtime

The AI Agent accepts natural language questions such as:

> "Why is Premium subscription churn increasing?"

The reasoning engine performs:

1. Understand the question
2. Select relevant tools
3. Generate SQL
4. Query Athena
5. Retrieve ML predictions
6. Combine evidence
7. Generate business insights
8. Return explainable results

Reasoning follows the **ReAct (Reason → Act → Observe)** pattern.

---

# 🤖 AI Components

## Tool Registry

The agent contains domain-specific analytics tools, including:

* Subscription Analytics
* User Analytics
* Revenue Analytics
* Churn Analytics
* Content Analytics
* Recommendation Analytics
* SQL Query Tool
* KPI Retrieval
* Metadata Lookup

The LLM dynamically selects the appropriate tools for each business question.

---

## Bedrock Brain

Powered by Amazon Bedrock.

Responsibilities:

* Natural language understanding
* Business reasoning
* Tool selection
* Insight generation

---

## Rule Engine

Handles deterministic business logic.

Examples:

* SQL validation
* Business constraints
* Guardrails
* Fallback execution

---

## Trace Engine

Every AI interaction produces a structured execution trace.

Each trace records:

* User question
* Selected tools
* SQL queries
* Tool outputs
* Intermediate reasoning
* Final response

This makes AI decisions transparent, reproducible, and auditable.

---

# 📊 Dashboard

The Streamlit dashboard provides:

* User Analytics
* Revenue Metrics
* Content Performance
* Churn Predictions
* Recommendations
* AI Chat Assistant
* Execution Traces
* Business KPIs

---

# ⚡ FastAPI

REST APIs expose:

* AI Question Answering
* Business Analytics
* ML Predictions
* Recommendation Results
* KPI Retrieval

---

# 📂 Project Structure

```text
data_generation/
    Synthetic OTT dataset generation

spark/
    Bronze/
    Silver/
    Gold/

dags/
    Apache Airflow workflows

ai_agent/
    AI reasoning engine
    Tool registry
    Bedrock integration
    Trace engine

dashboard/
    Streamlit dashboard

infra/
    AWS infrastructure

docker/
    Docker deployment

tests/
    Unit & integration tests

docs/
    Architecture & design documents
```

---

# ✨ Key Features

* Production-style Medallion Lakehouse
* Apache Kafka Streaming
* Apache Spark ETL Pipelines
* Delta Lake Storage
* Apache Airflow Orchestration
* Feature Engineering
* ML Prediction Pipelines
* Recommendation System
* AI Agent with LangGraph
* Amazon Bedrock Integration
* Athena SQL Analytics
* Streamlit Dashboard
* FastAPI Services
* Explainable AI Tracing
* Modular & Scalable Architecture

---

# 📄 Resume Highlights

* Designed and implemented an end-to-end Medallion Lakehouse architecture using Apache Spark, Delta Lake, Kafka, and AWS S3.
* Built real-time streaming ETL pipelines orchestrated with Apache Airflow.
* Developed predictive models for customer churn, content popularity, and personalized recommendations using Scikit-Learn and Spark MLlib.
* Engineered an AI-powered analytics assistant using LangGraph and Amazon Bedrock capable of answering business questions through dynamic tool calling.
* Integrated Amazon Athena for SQL analytics over Delta Lake and PostgreSQL for runtime metadata management.
* Developed interactive dashboards with Streamlit and REST APIs using FastAPI.
* Implemented explainable AI tracing to capture reasoning steps, SQL execution, tool invocations, and final responses for auditability.

---

# 🎓 Learning Outcomes

This project demonstrates hands-on experience with:

* Data Engineering
* Batch & Streaming ETL
* Medallion Lakehouse Architecture
* Apache Spark
* Apache Kafka
* Delta Lake
* Data Quality Engineering
* Analytics Engineering
* Machine Learning Pipelines
* Recommendation Systems
* AI Agents
* LangGraph
* Amazon Bedrock
* Cloud Data Platforms
* MLOps Fundamentals
* Production System Design

---

# 🛠 Tech Stack

**Languages:** Python 3.10+

**Data Engineering:** Apache Spark, Delta Lake, Apache Kafka, Apache Airflow

**Machine Learning:** Scikit-Learn, Spark MLlib (ALS)

**Cloud:** AWS S3, Amazon Athena, Amazon Bedrock, IAM

**Storage:** Delta Lake, PostgreSQL

**Backend:** FastAPI

**Frontend:** Streamlit, Plotly

**Testing:** pytest

---

This version is much closer to what a senior data engineer or ML engineer would expect to see on GitHub. It aligns with your architecture diagram and is suitable to discuss confidently in interviews.
