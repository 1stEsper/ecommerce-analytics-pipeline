# Ecommerce Analytics Pipeline

A scalable, containerized ETL pipeline that ingests ecommerce datasets, loads raw data into PostgreSQL, and computes analytics tables with Apache Spark—all orchestrated in Docker.

---

## 📦 Project Structure
```
ecommerce-analytics-pipeline/
├── data/ 
│   ├── raw/ #Raw data files (csv)
│   │   ├── olist_customers_dataset.csv
│   │   ├── olist_orders_dataset.csv
│   │   ├── olist_order_items_dataset.csv
│   │   └── olist_products_dataset.csv
│   ├── processed/ # Transformed data
│   ├── streaming/
│   └── warehouse/
├── src/
│   ├── extract/
│   │   └── data_loader.py # Loads CSVs to PostgreSQL 
│   ├── load/
│   └── transform/
│       └── spark_etl.py # ETL and analytics job
├── sql/
│   └── init/ # Database initialization
│       └── 01_create_schemas.sql 
├── docker-compose.yml # Service definitions
├── .env # Environment variables (DB creds)
└── README.md
```

---

## 🚀 Quick Start

### 1. Clone This Repository

```
git clone https://github.com/1stEsper/ecommerce-analytics-pipeline.git
cd ecommerce-analytics-pipeline
```

### 2. Place Data Files

Copy the raw Olist CSV files into `data/raw/`

### 3. Set Up Environment Variables

Create a `.env` file:
```
POSTGRES_DB=ecommerce_db
POSTGRES_USER=ecommerce_user
POSTGRES_PASSWORD=ecommerce_pass
```

### 4. Build and Start Docker Services
```
docker-compose up -d
```


Services started:
- PostgreSQL (`ecommerce_postgres`)
- pgAdmin for DB management (`ecommerce_pgadmin`)
- Redis (for streaming)
- Apache Spark Master & Workers
- Jupyter notebook server

Access the services: 
- Spark UI : http://localhost:8080
- Jupyter Lab : http://localhost:8888 
- PgAdmin : http://localhost:5050 (admin@admin.com / admin123)
- PostgresSQL : localhost:5432


### 5. Load Raw Data into PostgreSQL

Run the data loader (from the host):
```
docker exec ecommerce_spark_master spark-submit
--master spark://spark-master:7077
--deploy-mode client
/opt/spark-apps/extract/data_loader.py
```
- When u execute this code, u can go to localhost **Spark UI** if u see in the **Completed Applications** table, **State** is **FINISHED**, This confirms that the `data_loader.py` script was submitted to the Spark cluster and ran to completion without crashing. The process was successful from Spark's perspective.

- The next step is to verify if the data was successfully written to the PostgreSQL database (write some queries to test).

### 6. Run the ETL Pipeline
```
docker exec ecommerce_spark_master spark-submit
--master spark://spark-master:7077
--deploy-mode client
/opt/spark-apps/transform/spark_etl.py
```

---

## 🧩 What’s Inside?

- **data_loader.py**: Reads CSVs, loads to PostgreSQL (`raw_data` schema)
- **spark_etl.py**: Reads `raw_data`, computes key analytics, writes to `staging` and `analytics` schemas
- **docker-compose.yml**: All services defined for orchestration
- **src/**: All pipeline and helper code modularized

---

## 📊 Inspect Results

- Use pgAdmin (http://localhost:5050) or psql to inspect data, run queries, and export results:
  - `raw_data.customers`, `staging.customer_metrics`, `analytics.daily_sales`, etc.

---

## 📝 License

MIT License (see [LICENSE](LICENSE) file)

---

## ✨ Credits

- [Olist Ecommerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
