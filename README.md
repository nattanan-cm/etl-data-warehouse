# ETL with Data Warehouse

This project demonstrates an ETL (Extract, Transform, Load) pipeline using Apache Airflow, MinIO as a data lake, and PostgreSQL as the data warehouse. Each component runs in a Docker container, enabling a streamlined, local development environment for data engineering tasks.

## Project Overview

1. **Airflow** orchestrates the ETL process, handling task scheduling and execution.
2. **MinIO** serves as the data lake, providing object storage for raw data in CSV format.
3. **PostgreSQL** functions as the data warehouse, storing transformed and structured data for analysis.

### Components

- **Airflow**: Manages ETL jobs and schedules.
- **MinIO**: Acts as the source layer where raw data is stored.
- **PostgreSQL**: Stores transformed data in a structured format for data warehousing.

## Prerequisites

Before starting, make sure your environment meets the following requirements:

1. **Docker and Docker Compose**

   - Install Docker: [Docker Installation Guide](https://docs.docker.com/get-docker/)
   - Install Docker Compose: [Docker Compose Installation Guide](https://docs.docker.com/compose/install/)

2. **Python 3.8+** (Optional, for Airflow CLI or local development)

   - Install Python: [Python Installation Guide](https://www.python.org/downloads/)

3. **Environment Setup**

   - Create an `.env` file in the project root directory and add the necessary environment variables:

     ```plaintext
     AIRFLOW_UID=<AIRFLOW_UID>
     MINIO_ACCESS=minioadmin
     MINIO_SECRET=miniopwd
     MINIO_ENDPOINT=minio:9000
     _PIP_ADDITIONAL_REQUIREMENTS=minio requests pandas numpy apache-airflow-providers-amazon apache-airflow-providers-postgres
     ```

4. **Data for Testing**
   
   - Prepare some sample CSV files (or use provided sample data) to test the ETL pipeline.
   - You can find example CSV files in the ./examples/ directory.

## Installation Steps

1. **Clone the Repository**
    ```bash
    git clone https://github.com/nattanan-cm/etl-data-warehouse.git
    cd etl-data-warehouse
    ```

2. **Start Services**

   Run the following command to start all services (Airflow, MinIO, PostgreSQL) using Docker Compose:

    ```bash
    docker-compose up airflow-init
    ```
    ```bash
    docker-compose up -d
    ```
    
## Workflow

1. **Extraction**: Airflow pulls CSV files from the `bronze` bucket in MinIO.
2. **Transformation**: Data transformations are applied, such as typecasting and cleaning.
3. **Loading**: The transformed data is loaded into PostgreSQL tables.

## Data Flow

1. **Bronze (Raw Data)** - Stored in MinIO, e.g., `minio/bucket/bronze`.
2. **Silver (Cleaned Data)** - Intermediate data after transformations.
3. **Gold (Data Warehouse)** - Final structured data in PostgreSQL.

## Configuration

1. **Airflow Connections**:
   - Create an Airflow connection for MinIO (S3-compatible) and PostgreSQL.
   
2. **Environment Variables**:
   - Update `.env` to match your setup.
   - Update `docker-compose.yml` to match your setup.

