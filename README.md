 # JioHotstar Media-Streaming Analytics 

# 🎬 AI-Powered Streaming Analytics Platform
### Production-Style Data Engineering + MLOps + AI Agent using AWS, Spark, Kafka & Bedrock


<img width="1693" height="929" alt="ChatGPT Image Jul 21, 2026, 02_55_10 PM" src="https://github.com/user-attachments/assets/5cb443e4-9055-419d-a9be-18f1f083f2a3" />



# Overview

This project simulates the backend data platform of a large OTT streaming company such as Netflix, Disney+, or JioHotstar.

Instead of building only machine learning models, this project demonstrates how modern enterprise data systems collect, process, analyze, predict, and explain business insights using an AI Agent.

The platform combines

- Data Engineering
- Medallion Lakehouse
- Real-time Streaming
- Machine Learning
- AI Agents
- AWS Cloud
- Interactive Dashboards

into one complete production-style architecture.

---

# Business Problem

Streaming platforms generate millions of events every day.

Examples include

- User logins
- Video plays
- Watch duration
- Ratings
- Subscription purchases
- Search history
- Device information
- Campaign clicks

Business teams need answers like

- Why are users cancelling subscriptions?
- Which content is trending?
- Which users are likely to churn?
- Which plan generates maximum revenue?
- What should be recommended next?

Instead of manually running SQL queries, an AI Agent automatically understands business questions and retrieves answers from analytics tables and ML models.

---

# High Level Architecture

Event Generator

↓

Kafka Streaming

↓

Spark Bronze Layer

↓

Spark Silver Layer

↓

Spark Gold Analytics Layer

↓

Machine Learning Models

↓

Athena + PostgreSQL

↓

AI Reasoning Runtime

↓

Streamlit Dashboard / FastAPI / CLI

---

# Technology Stack

## Cloud

- AWS S3
- AWS Athena
- Amazon Bedrock
- IAM

---

## Data Engineering

- Apache Spark
- Delta Lake
- Apache Kafka
- Apache Airflow

---

## Machine Learning

- Scikit-learn
- Random Forest
- ALS Recommendation Engine

---

## Backend

- Python
- FastAPI

---

## AI

- LangGraph
- Amazon Bedrock
- Tool Calling
- AI Agent
- Rule Engine

---

## Storage

- PostgreSQL
- Delta Lake
- S3

---

## Frontend

- Streamlit

---

# Project Workflow

## Phase 1

Data Generation

Synthetic streaming data is generated for

- Users
- Subscriptions
- Ratings
- Content
- Events
- Campaigns

These simulate production traffic.

---

## Phase 2

Kafka Streaming

User events are continuously published into Kafka.

Examples

- Watch Started
- Pause
- Completed
- Search
- Subscription Purchased

---

## Phase 3

Bronze Layer

Spark Streaming consumes Kafka messages.

Raw events are stored without modification inside Delta Lake Bronze tables.

Purpose

- Raw backup
- Audit
- Replay capability

---

## Phase 4

Silver Layer

Spark performs

- Data Cleaning
- Schema Validation
- Null Handling
- Deduplication
- Type Conversion
- Business Rules
- Quality Checks

Output becomes trusted datasets.

---

## Phase 5

Gold Layer

Business analytics tables are created.

Examples

- Daily Active Users
- User Engagement
- Subscription Metrics
- Genre Popularity
- Content Ratings
- Watch Statistics

These tables are optimized for dashboards and AI queries.

---

## Phase 6

Machine Learning

Three ML systems are trained.

### Customer Churn Prediction

Algorithm

Random Forest

Predicts

Probability that a customer cancels subscription.

---

### Content Popularity Prediction

Algorithm

Random Forest

Predicts

Future trending content.

---

### Recommendation System

Algorithm

ALS Collaborative Filtering

Recommends personalized content.

Predictions are written back into Gold tables.

---

## Phase 7

Storage Layer

Business analytics tables

↓

AWS Athena

Machine learning outputs

↓

PostgreSQL

The AI Agent reads from both systems.

---

## Phase 8

AI Reasoning Runtime

The AI Agent receives natural language questions.

Example

"Why is Premium churn increasing?"

The reasoning engine

1. Understands the question

2. Chooses required analytics tools

3. Executes SQL

4. Retrieves ML predictions

5. Collects evidence

6. Produces business explanation

The agent follows a

Plan

↓

Act

↓

Observe

↓

Reason

workflow.

---

# AI Components

## Tool Registry

Contains strongly typed business tools such as

- Churn Analytics
- Revenue Analytics
- Content Analytics
- Recommendation Analytics
- SQL Query Tool
- User Analytics

The AI dynamically selects tools.

---

## Bedrock Brain

LLM powered reasoning.

Responsibilities

- Natural Language Understanding
- Tool Selection
- Explanation Generation
- Business Insights

---

## Rule Brain

Rule-based deterministic engine.

Responsibilities

- SQL Validation
- Business Constraints
- Guardrails
- Offline Decision Making

---

## Trace Engine

Every reasoning step is recorded.

Each trace stores

- User Question
- Selected Tools
- SQL Executed
- Tool Outputs
- Final Answer

This makes the AI Agent explainable and reproducible.

---

# Dashboard

The Streamlit application displays

- User Analytics
- Revenue Metrics
- Churn Prediction
- Recommendation Results
- AI Reasoning Trace
- Business KPIs

---

# FastAPI

FastAPI exposes REST APIs for

- Ask AI
- Execute Analytics
- Retrieve Predictions
- Query Business Metrics

---

# Folder Structure

```
data_generation/
    Generate demo datasets

spark/
    Bronze
    Silver
    Gold

dags/
    Airflow orchestration

ai_agent/
    AI reasoning runtime

dashboard/
    Streamlit dashboard

config/
    Configuration

infra/
    AWS deployment

docker/
    Docker deployment

tests/
    Unit tests

docs/
    Design documents
```

---

# Key Features

✔ Production-style Medallion Lakehouse

✔ Kafka Real-time Streaming

✔ Apache Spark ETL

✔ Delta Lake

✔ Airflow Orchestration

✔ Machine Learning Pipeline

✔ Recommendation Engine

✔ AI Agent with Tool Calling

✔ Amazon Bedrock Integration

✔ Athena Analytics

✔ PostgreSQL

✔ Streamlit Dashboard

✔ FastAPI Backend

✔ Explainable AI Traces

---

# Resume Highlights

• Designed an end-to-end Medallion Lakehouse architecture using Apache Spark, Delta Lake, Kafka, and AWS S3.

• Built scalable ETL pipelines orchestrated using Apache Airflow.

• Developed ML models for churn prediction, content popularity prediction, and personalized recommendations.

• Implemented an AI Agent using LangGraph and Amazon Bedrock capable of reasoning over business analytics through dynamic tool calling.

• Integrated Athena and PostgreSQL as analytical query engines for AI-assisted decision making.

• Built interactive dashboards using Streamlit and REST APIs using FastAPI.

• Implemented explainable AI tracing to record reasoning steps, tool execution, SQL queries, and final responses for reproducibility.

---

# Learning Outcomes

This project demonstrates practical knowledge of

- Data Engineering
- ETL Pipelines
- Lakehouse Architecture
- Streaming Systems
- Machine Learning
- Recommendation Systems
- AI Agents
- Cloud Architecture
- MLOps
- Analytics Engineering
- Production System Design

## Tech stack

Python 3.10 · pandas · Apache Spark + Delta Lake · Apache Kafka · Apache
Airflow · PostgreSQL · AWS S3 · AWS Athena · AWS Bedrock · FastAPI · Streamlit
· Plotly · pytest.
