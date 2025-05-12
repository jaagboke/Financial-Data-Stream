# Financial-Data-Stream

# Introduction
This project implements a real-time financial data streaming pipeline utilising **Apache Kafka, Apache Spark, MinIO (S3-compatible object store), ClickHouse and Apache Airflow. ** Everything is containerized using docker.


# System Architecture
![image](https://github.com/user-attachments/assets/8591cf2e-2da8-489f-9055-6db73badf49a)

- **Data Source**: Faker is used to generate faker data
- **Apache Airflow**: Orchestrates the entire pipeline by scheduling and running DAGs that manage the data streaming and processing workflow
- **Apache kafka**: Used to stream financial transaction data in real time.
- **Minio**: Serves as an S3-compatible object store where the parquet files are stored for further analysis.
- **PowerBi**: Used to visualise data.
- **ClickHouse**: Reads the parquet files directly from Minio using its native S3() table function.
- **Docker**: Ensures all components run consistently across different environments.
