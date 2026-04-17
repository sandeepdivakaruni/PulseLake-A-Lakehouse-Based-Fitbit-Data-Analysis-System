# PulseLake: A Lakehouse-Based Fitbit Data Analysis System

## Overview
PulseLake is an end-to-end lakehouse data engineering project that ingests, processes, and analyzes Fitbit-style wearable data using batch + streaming pipelines.

The platform combines:
- Apache Kafka for real-time event streaming
- Azure Data Factory for incremental batch ingestion
- Azure Databricks (PySpark) for transformations
- Delta Lake with Medallion Architecture (Bronze, Silver, Gold)
- Power BI for analytics dashboards

The system transforms raw wearable data (heart rate, workouts, gym activity) into analytics-ready datasets and interactive dashboards.

---

## Architecture
Pipeline flow:

Source Systems  
↓  
Kafka (Streaming) + Azure SQL (Batch)  
↓  
Azure Data Factory + Landing Zone  
↓  
Bronze Layer (Raw Delta Tables)  
↓  
Silver Layer (Cleaning, Deduplication, MERGE Upserts)  
↓  
Gold Layer (Aggregated Analytics Tables)  
↓  
Power BI Dashboards

---

## Tech Stack
- Python
- PySpark
- Confluent Kafka
- Azure Data Factory
- Azure Databricks
- Delta Lake
- Azure Data Lake Storage Gen2
- Azure SQL Database
- Power BI

---

## Features
- Hybrid Batch + Streaming ingestion
- Medallion architecture implementation
- CDC and MERGE-based upserts
- Workout session reconstruction
- Heart-rate analytics
- Fault-tolerant structured streaming
- Gold-layer dashboard reporting

---

## Datasets
Synthetic Fitbit-style datasets include:
- User profiles
- Workout events
- BPM (heart rate) events
- Registered users
- Gym login activity

---
## System Architecture
<img width="684" height="320" alt="architecture png" src="https://github.com/user-attachments/assets/369ef146-75a5-42ec-8359-2b29501c7f5c" />

---
## Power BI Dashboards
Includes:
<div align="center">

<img src="https://github.com/user-attachments/assets/59390529-5b87-4533-9cdd-647da9218a63" width="48%">
<img src="https://github.com/user-attachments/assets/ee64e191-0630-4d77-9120-aa8c6736d732" width="48%">

</div>

<br>

<div align="center">

<img src="https://github.com/user-attachments/assets/453401ba-bd78-4ecc-9db7-0aef98f2ea5f" width="48%">
<img src="https://github.com/user-attachments/assets/b0f22ab3-c353-40b4-a3de-f9886b922cf4" width="48%">

</div>

<br>

<div align="center">

<img src="https://github.com/user-attachments/assets/749124e1-cb58-454f-8804-73b8cb8b5245" width="48%">

</div>

---

## Key Data Engineering Concepts Demonstrated
- Structured Streaming
- Watermarking
- Checkpointing
- Delta MERGE
- CDC Processing
- Medallion Architecture
- Lakehouse Design
- Data Modeling

---

## Project Outcomes
- Processed millions of synthetic BPM records
- Built scalable Bronze-Silver-Gold data pipeline
- Generated analytics-ready Delta tables
- Delivered interactive business intelligence dashboards

---

## Future Improvements
- Integrate real Fitbit APIs
- Add anomaly detection
- Automate streaming jobs
- Real-time dashboard refresh
- Add machine learning predictions

---
