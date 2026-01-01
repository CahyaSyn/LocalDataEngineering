# LOCAL DATA ENGINEERING INTEGRASI DENGAN AIRFLOW

## 📌 Project Overview

Project ini untuk mendemonstrasikan bagaimana untuk membangun **end to end ELT workflow** dengan mempraktikkan:

- Incremental ingestion
- Bronze / Silver Layer
- Data quality check
- Indepotent schema dan constraint management
- Orchestrasion menggunakan Apache Airflow 3.1.5

## 🏗️ Architecture
```
Raw Source (CSV / Mock Data)
            │
            ▼
RAW Layer (raw.transactions_raw)
            │
            ▼
BRONZE Layer (bronze.transactions)
            │
            ▼
SILVER Layer (silver.sales)
            │
            ▼
(Optional) GOLD Layer (aggregations / analytics)
```

## 🧰 Tech Stack
| Component       | 	Technology           |
|-----------------|-----------------------|
| Language        | 	Python 3.12          |
| Database        | 	PostgreSQL 17        |
| ORM / SQL       | 	SQLAlchemy (Core)    |
| Orchestration   | 	Apache Airflow 3.1.5 |
| Environment     | 	Linux (recommended)  |
| Version Control | 	Git & GitHub         |


## 📁 Project Structure
```text
LocalDataEngineer/
│
├── etl/
│ │── raw_ingestion.py
│ ├── bronze_transform.py
│ ├── silver_transform.py
│ ├── gold_transform.py
│ ├── data_quality_check.py
│ ├── db_connection.py
│ └── run_all.py
│
├── airflow/
│ ├── dags/
│ │ └── elt_pipeline_dag.py
│ └── airflow.db
│
├── .env.example
├── requirements.txt
└── README.md
```

## ⚙️ Environment Setup
### Create Virtual Environtment
```text
python -m venv .venv
source .venv/bin/activate
```

### Install Dependencies
```text
pip install 'apache-airflow[postgres,google]==3.1.5' \
 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.5/constraints-3.10.txt"
pip install -r requirements.txt
```

### Create new database
```text
CREATE DATABASE yourdatabase;
```

### Create Schema
```text
CREATE SCHEMA raw;
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
```

### Run Pipeline Local (Tanpa Airflow)
```text
python etl/run_all.py
```

### 🪨 Bronze Layer

#### Purpose
- Clean raw data
- Deduplicate records
- Enforce schema & constraints

#### Key Features
- ***CREATE TABLE IF NOT EXISTS***
- Conditional ***CHECK*** constraints
- Deduplication menggunakan ***ROW_NUMBER()***
- Incremental insert with conflict handling

### 🥈 Silver Layer

#### Purpose
- Business-ready data
- Stable analytical schema

#### Key features
- ***total_price = price * quantity***
- Type casting & normalization
- ***UPSERT logic (ON CONFLICT DO UPDATE)***
- Data quality enforcement

### ✅ Data Quality Check

Mengimplementasikan SQL-based assertion

- Tidak boleh negative (total price)
- Tidak boleh NULL pada kolom penting
- Row-level validation
- Pipeline gagal jika quality check tidal lolos

### ⏱️ Orchestration with Airflow

#### Menjalankan Airflow(Linux)
```text
airflow db migrate
airflow standalone
```

#### Fitur DAG

- Task-level bisa di retry
- Rantai dependency yang jelas
- Bisa dijalankan berulang