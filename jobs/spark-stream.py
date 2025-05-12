#Import the necessary modules
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import StringType, StructType, from_json, col, min, max
from pyspark.sql.types import StructField, TimestampType, FloatType
from config import configuration
import traceback

#Define the data schema
def data_schema():
    schema = StructType([
        StructField('transaction_id', StringType(), False),
        StructField('timestamp', TimestampType(), False),
        StructField('sender_id', StringType(), False),
        StructField('receiver_id', StringType(), False),
        StructField('amount', StringType(), False),
        StructField('currency', StringType(), False),
        StructField('longitude', StringType(), False),
        StructField('latitude', StringType(), False),
        StructField('device_id', StringType(), False),
        StructField('transaction_type', StringType(), False)
    ])

    return schema


#Read the data stream from Kafka
def readKafka(schema, topic, spark):
    try:
        print(f"Connecting to Kafka topic: {topic}")
        print(f"Using bootstrap servers: {configuration['bootstrap.servers']}")
        print(f"Using security protocol: {configuration['security.protocol']}")

        # Load Kafka stream
        lines = (
            spark.readStream
            .format('kafka')
            .option("kafka.bootstrap.servers", configuration['bootstrap.servers'])
            .option("kafka.security.protocol", configuration['security.protocol'])
            .option("kafka.sasl.mechanism", configuration['sasl.mechanisms'])
            .option("kafka.sasl.jaas.config",
                    f'org.apache.kafka.common.security.plain.PlainLoginModule required '
                    f'username="{configuration["sasl.username"]}" '
                    f'password="{configuration["sasl.password"]}";')
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .option("kafka.ssl.endpoint.identification.algorithm", "https")
            .option("kafka.ssl.truststore.type", "PEM")
            .load()
        )

        # Print schema to verify if data is loaded
        lines.printSchema()

        lines = lines.selectExpr('CAST(value AS STRING)')
        lines = lines.select(from_json(col('value'), schema).alias('data'))
        lines = lines.select('data.*')

        return lines
    except Exception as e:
        logging.error(f"Error reading from Kafka: {traceback.format_exc()}")
        raise e

#Transform the kafka data
"""
- Cast "amount" as double
- Cast "longitude" as double
- Cast "latitude" as double
"""
def transform_kafka_data(df):
    df = df.withColumn("amount", col("amount").cast("double")) \
        .withColumn("longitude", col("longitude").cast("double")) \
        .withColumn("latitude", col("latitude").cast("double"))
    return df

#Grab the bucket name
source_bucket = configuration['SOURCE_BUCKET']

#Definbe the output Path
output_path = f"s3a://{source_bucket}/finance_stream"

#Write the Kafka stream to the Minio S3 bucket
def writeKafka(df):
    results = (
        df.writeStream
        .format('parquet')
        .outputMode("append")
        .option("path", output_path)
        .option("truncate", "false")
        .option('checkpointLocation', f"s3a://{source_bucket}/checkpoints")
        .start()
    )
    return results

if __name__ == '__main__':

    #Specify the kafka topic
    KAFKA_TOPIC = 'financial_data'

    #Initialise the Spark Connection
    """
    Specify the configs to access the Minio Access & Secret keys
    """
    try:
        spark_conn = (SparkSession.builder
                  .appName('KafkaToConsole')
                  .config("spark.jars.packages",
                          "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
                          "org.apache.kafka:kafka-clients:3.4.1")

                      .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
                      .config("spark.hadoop.fs.s3a.access.key", configuration['MINIO_ACCESS_KEY'])
                      .config("spark.hadoop.fs.s3a.secret.key", configuration['MINIO_SECRET_KEY'])
                      .config("spark.hadoop.fs.s3a.path.style.access", "true")
                      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                      .getOrCreate())
        spark_conn.sparkContext.setLogLevel('WARN')

        """Create The Data Schema"""
        schema = data_schema()

        """Read Kafka Stream"""
        data = readKafka(schema, 'financial_data', spark_conn)

        """Transform Data"""
        transform_df = transform_kafka_data(data)

        """Write Kafka Stream To Console"""
        results = writeKafka(transform_df)

        """Await Termination"""
        results.awaitTermination()
    except Exception as e:
        logging.error(f"DATA NOT STREAMED!\n%s", traceback.format_exc())
        traceback.print_exc()