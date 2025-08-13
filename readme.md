# Project Architecture
![1010 drawio](https://github.com/user-attachments/assets/f221d5df-98ed-4a5a-8574-845b188cd23c)

# Demo
https://www.youtube.com/watch?v=r1FkifkOdvA
# Lambda Architecture Data Processing Platform

This project implements a Lambda Architecture for data processing, combining both batch and real-time data streams to provide a unified view for analysis and reporting. The project utilizes a hybrid approach, with the batch processing layer running on the cloud and the speed processing layer running locally.

## Architecture Overview

The project is structured around the Lambda Architecture paradigm, consisting of the following layers:

*   **Batch Layer (Cloud-Based):** Processes large datasets from historical data sources using cloud resources.
*   **Speed Layer (Local):** Processes real-time data streams locally to provide low-latency insights.
*   **Serving Layer:** Provides a unified view of processed data for analysis and reporting.

## Data Flow

### Batch Data

1.  **Data Source:** Data is extracted from an on-premise Microsoft SQL Server database.
2.  **Ingestion:** Data is ingested into Azure Data Lake Gen2 using Azure Data Factory.
3.  **Transformation:** Data is processed and transformed using Azure Databricks. This includes filtering, cleaning, augmentation and aggregations.
4.  **Analysis:** Processed data is loaded to Azure Synapse Analytics for analysis.
5.  **Visualization:** Data is visualized using Power BI for reporting.

### Real-time Data

1.  **Data Source:** Data streams from a real-time API endpoint.
2.  **Ingestion:** Streaming data is processed by custom components and routed by Apache Airflow.
3.  **Storage:** The streaming data is stored in a PostgreSQL database.
4.  **Processing:** Data is further processed using Apache Spark and Jupyter.
5.  **Warehouse:** Processed data is persisted in Apache Cassandra for consumption.
6.  **Visualization:** Processed data from the batch and speed layers is unified and consumed through visualizations in Power BI.

## Components

### Batch Processing Layer (Azure Cloud)

*   **Azure Integration Runtime:** Facilitates data movement between on-premise and Azure.
*   **Azure Data Factory:** Manages the data pipeline for batch ingestion.
*   **Azure Data Lake Gen2:** Scalable data lake for storing raw, processed, and aggregated data:
    *   **Bronze:** Raw data.
    *   **Silver:** Filtered and cleaned data.
    *   **Gold:** Business-level aggregated data.
*   **Azure Databricks:** Platform for running large-scale data transformations and analytics.
*   **Azure Synapse Analytics:** Used for data analysis and creating interactive dashboards.
*   **Power BI:** Data visualization and reporting platform.
*   **Azure Active Directory:** User authentication.
*   **Azure Key Vault:** Secure storage of secrets.

### Speed Processing Layer (Local)

*   **API (Local):** Real-time data input from custom application or service.
*   **Python (Local):** Custom Python application for initial data processing.
*   **Apache Airflow (Local):** Orchestration of local data processing workflows.
*   **PostgreSQL (Local):** Local data store for streaming data.
*   **Custom Data Processing Components (Local):** Set of custom tools for specific data transformations and quality monitoring.
*   **Apache Spark (Local):** Parallel processing engine for complex local transformations.
*   **Jupyter Notebook (Local):** Platform to build and visualize results with Apache Spark.
*   **Apache Cassandra (Local):** Data warehouse for locally processed streaming data.
*   **Docker (Local):** Containerization of the entire speed layer.

## Getting Started

### Prerequisites

*   An Azure account with access to necessary services.
*   Docker installed locally.
*   Python 3.x.
*   Apache Spark and Jupyter setup in local environment.
*   PostgreSQL and Cassandra setup in local environment.

### Setup Instructions

1.  **Cloud Resources:**
    *   Provision necessary Azure resources such as Data Factory, Data Lake, Databricks, Synapse, and Power BI.
    *   Configure Azure Integration Runtime for on-premise connectivity.

2.  **Local Environment:**
    *   Clone the repository
        ```bash
        git clone https://github.com/younes921722/Real-Time-Data-Pipeline.git
        ```
    *   Build and run docker containers
        ```bash
        docker-compose up -d
        ```
    *   Configure Python environment with necessary libraries.

3.  **Data Pipelines:**
    *   Configure Azure Data Factory pipeline to ingest data from SQL Server.
    *   Develop necessary data transformations in Azure Databricks.

4.  **Local Processing:**
    *   Write necessary Python scripts for initial data processing.
    *   Configure Airflow workflows for data routing and processing.
    *   Configure the Spark application.
    *   Establish data connections between components.
    *   Deploy the docker container and related scripts locally.

5.  **Visualization:**
    *   Create Power BI dashboards to consume data from Azure Synapse and other sources.

## Technology Stack

*   **Cloud:** Azure Data Factory, Azure Data Lake Gen2, Azure Databricks, Azure Synapse Analytics, Power BI, Azure Active Directory, Azure Key Vault.
*   **Local:** Python, Apache Airflow, PostgreSQL, Apache Spark, Jupyter, Apache Cassandra, Docker.

## Contributing

Please feel free to submit issues, feature requests, and pull requests. We welcome contributions from the community.
