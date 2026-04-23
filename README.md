# Hospital Patient Flow & Bed Occupancy Analytics

# Azure Healthcare Real-Time Data Pipeline
### Patient Flow & Bed Occupancy Analytics Platform

> **Simulated project** inspired by **Data with Jay** for Midwest Health Alliance (MHA) a fictional network of 7 hospitals requiring a centralized real-time data platform. Not a real hospital.

---

## 📌 Project Summary

I built a fully automated, end-to-end real-time healthcare data pipeline on Microsoft Azure. The pipeline ingests simulated patient admission and discharge events, processes them through a Medallion Architecture (Bronze → Silver → Gold), and models the data into a Star Schema.

 Data Engineering:** Real-time ingestion + transformation pipeline (Event Hubs → Databricks → ADLS → Synapse)



---

## ✅ Key Outcomes

- **End-to-End Pipeline:** From real-time ingestion → transformation → warehouse → analytics
- **Scalable Architecture:** Easily adaptable for different hospital datasets
- **Business Insights:** Hospital admins can monitor bed usage, patient flow, and department efficiency in real time
- **Portfolio Value:** Demonstrates both Data Engineering skills in this project

---

## 🏗️ Pipeline Architecture

```
patient-flow-generator.py  (Local PC — simulates EMR events)
        ↓
Azure Event Hubs  (Kafka Protocol — real-time ingestion)
        ↓
Azure Databricks — Bronze Layer  (raw Delta storage)
        ↓
Azure Databricks — Silver Layer  (cleaned & validated data)
        ↓
Azure Databricks — Gold Layer    (Star Schema, business-enriched data)
        ↓
Azure Synapse Analytics          (Serverless SQL Built-in)

```

---

## 🎯 Objectives

- Collect real-time patient data via Azure Event Hubs
- Process and cleanse data using Databricks (Bronze → Silver → Gold layers)
- Implement a Star Schema in Synapse for efficient querying
- Create a connection end point to data analyst to connect to Power BI for live hospital KPI dashboards
- Enable Version Control with Git

---

## 🛠️ Technology Stack

| Layer | Azure Service | Tool / Protocol | Purpose |
|---|---|---|---|
| Data Ingestion | Azure Event Hubs | Kafka Protocol | Real-time patient event streaming |
| Stream Processing | Azure Databricks | PySpark / Delta Lake | Bronze layer raw data processing |
| Data Cleaning | Azure Databricks | PySpark | Silver layer transformation & quality |
| Data Modeling | Azure Databricks | PySpark / SCD Type 2 | Gold layer star schema & dimensions |
| Orchestration | Azure Data Factory | ADF Pipelines | Automated batch & streaming triggers |
| Analytics | Azure Synapse Analytics | Serverless SQL (Built-in) | SQL queries on gold Delta tables |
| Security | Azure Key Vault | Managed Identity | Secrets & credentials management |
| Storage | Azure Data Lake Gen2 | ADLS / Delta Format | Bronze, Silver, Gold containers |
| Version Control | GitHub | Git | Code, notebooks & SQL scripts |

---

## 📂 Repository Structure

```
azure-healthcare-realtime-data-pipeline/
│
├── databricks-notebooks/
│   ├── bronze_hospital_raw.ipynb           # Raw ingestion from Event Hubs
│   ├── silver_hospital_cleandata.ipynb     # Data cleaning & validation
│   └── gold_hospital_enricheddata.ipynb   # Star schema & business enriched data
│
├── event-hub-generator/
│   └── patient-flow-generator.py           # Simulates live EMR events
│
├── sql-pool-queries/
│   └── SQL_Synapse_pool_queries.txt        # Synapse external table creation
│
├── adf-pipelines/
│   └── pipeline.json                       # ADF pipeline export
│
└── README.md
```

---

## 🥉🥈🥇 Medallion Architecture

### Bronze Layer — Raw Zone
- Raw patient events ingested from Azure Event Hubs stored as-is in Delta format
- No transformation applied — full history preserved
- **Path:** `abfss://container@StorageAccountName.dfs.core.windows.net/path`

### Silver Layer — Cleaned Zone
- Deduplicated, validated and transformed data
- Handles dirty data: missing timestamps, duplicate patient IDs, wrong data types, null values
- Schema evolution handled using Delta Lake `mergeSchema` option
- **Path:** `abfss://container@StorageAccountName.dfs.core.windows.net/path`

### Gold Layer — Enriched Zone
- Star Schema dimensional model ready for analytics
- SCD Type 2 implemented for patient and department history tracking
- **Path:** `abfss://container@StorageAccountName.dfs.core.windows.net/`

---

## ⭐ Data Model — Star Schema (Gold Layer)

```
                    ┌─────────────────┐
                    │   dim_patient   │
                    │─────────────────│
                    │ surrogate_key   │
                    │ patient_id      │
                    │ gender          │
                    │ age             │
                    │ effective_from  │
                    │ effective_to    │
                    │ is_current      │
                    └────────┬────────┘
                             │
┌──────────────────┐  ┌──────┴──────────────┐
│  dim_department  │  │  fact_patient_flow  │
│──────────────────│  │─────────────────────│
│ surrogate_key    ├──│ fact_id             │
│ department       │  │ patient_sk          │
│ hospital_id      │  │ department_sk       │
└──────────────────┘  │ admission_time      │
                      │ discharge_time      │
                      │ length_of_stay_hrs  │
                      │ is_currently_admitted│
                      └─────────────────────┘
```

### SCD Type 2 — Patient Dimension
Every time a patient record changes, the old record is expired and a new one is inserted preserving full history:
- `is_current = True` → active/latest record
- `is_current = False` → expired/historical record
- `effective_from` → when the record became active
- `effective_to` → when the record expired (NULL if still active)

---

## ⚙️ Step-by-Step Implementation

### 1. Event Hub Setup
- Created Event Hub namespace and `patient-flow` hub
- Configured consumer groups for Databricks streaming

### 2. Data Simulation
- Developed Python script `patient-flow-generator.py` and run in my local CMD to stream fake patient data (departments, wait time, discharge status) to Event Hub via Kafka protocol
- 
### 3. Storage Setup
- Configured Azure Data Lake Storage Gen2
- Created containers: `bronze`, `silver`, `gold`

### 4. Databricks Processing
- **Notebook 1:** Reads Event Hub stream into Bronze layer
- **Notebook 2:** Cleans and validates schema in Silver layer
- **Notebook 3:** Aggregates and prepares Star Schema tables in Gold layer

### 5. Synapse Analytics
- Connected to Synapse Serverless SQL (Built-in)
- Created External Data Source, File Format and External Tables
- Queried `dim_patient`, `dim_department`, `fact_patient_flow`


### 6. Version Control
- All code, notebooks and SQL scripts pushed to GitHub

---

## 🔒 Security & Compliance

- **Azure Key Vault** stores all secrets — storage access keys, Databricks tokens, connection strings
- **Managed Identity** used for Synapse to ADLS authentication — no hardcoded credentials
- **Database Scoped Credentials** created in Synapse for secure data source access
- **Role-Based Access Control (RBAC)** applied across all Azure services
- **All data encrypted** in transit (SASL_SSL) and at rest in Azure Data Lake
- **Patient IDs hashed** using SHA-256 — simulating HIPAA-compliant architecture

---

## ⚙️ Orchestration — Azure Data Factory

- Daily batch ingestion triggers from simulated EHR systems
- Real-time processing triggers connected to Databricks notebooks
- Get Metadata activity validates file existence before processing
- If Condition activity routes pipeline based on data availability
- Pipeline failure alerts configured

---

## 🚀 How to Run

### Step 1 — Generate & Stream Data
```bash
# Install kafka-python
pip install kafka-python

# Run the patient flow generator from your local machine
python patient-flow-generator.py
```

### Step 2 — Process in Databricks
1. Run `bronze_hospital_raw.ipynb` — ingests from Event Hubs to Bronze layer
2. Run `silver_hospital_cleandata.ipynb` — cleans and validates to Silver layer
3. Run `gold_hospital_enricheddata.ipynb` — builds Star Schema in Gold layer

### Step 3 — Query in Synapse
```sql
-- Connect to Synapse Built-in (Serverless SQL)
-- Run SQL_Synapse_pool_queries.txt to create external tables
SELECT * FROM dbo.dim_patient
SELECT * FROM dbo.dim_department
SELECT * FROM dbo.fact_patient_flow
```

## 💡 Key Learnings

- Newer Databricks workspaces have DBFS disabled — use ADLS paths (`abfss://`) for all checkpoints
- Synapse External Tables read Parquet by column position — order must match Databricks exactly
- `OPENROWSET` only works on Synapse Serverless (Built-in) — not on Dedicated Pool
- SCD Type 2 requires two-step merge — expire old records first, then insert new ones
- Delta Lake `mergeSchema` option prevents pipeline downtime during schema evolution
- Azure Key Vault secret scopes must be configured in Databricks before use

---

## 📋 Business Requirements Addressed

| Requirement | Status |
|---|---|
| Real-time patient admission monitoring | ✅ Implemented |
| Department-level bottleneck identification | ✅ Implemented |
| Medallion Architecture (Bronze → Silver → Gold) | ✅ Implemented |
| SCD Type 2 for patient & department history | ✅ Implemented |
| Star Schema for analytics | ✅ Implemented |
| Azure Synapse for SQL analytics | ✅ Implemented |
| ADF pipeline orchestration & automation | ✅ Implemented |
| Data quality handling (dirty data) | ✅ Implemented |
| Azure Key Vault security | ✅ Implemented |

---

## 👤 Author

**Donatus Victor**
- GitHub: [azure-healthcare-realtime-data-pipeline](https://github.com)
- Built: April 2026

---

> *This is a portfolio project built to demonstrate end-to-end Azure data engineering skills. All patient data is simulated and does not represent any real individuals or hospital.*

