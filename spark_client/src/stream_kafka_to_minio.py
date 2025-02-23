import json
import time
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, FloatType

# Load table schema from JSON configuration file
def load_table_schema(file_path):
    with open(file_path, "r") as file:
        schema_json = json.load(file)
    
    def parse_field(field):
        field_type = field["type"]
        if field_type == "string":
            return StructField(field["name"], StringType())
        elif field_type == "int":
            return StructField(field["name"], IntegerType())
        elif field_type == "long":
            return StructField(field["name"], LongType())
        elif field_type == "float":
            return StructField(field["name"], FloatType())
    
    return StructType([parse_field(f) for f in schema_json["columns"]])

# Load table schema from configuration file
schema_file = "schema_config.json"
table_schema = load_table_schema(schema_file)

# Initialize MinIO Client
minio_client = boto3.client(
    's3',
    endpoint_url="http://minio:9000",  
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
    region_name='us-east-1'
)

# Create a bucket in MinIO if it does not exist
bucket_name = "customers"
existing_buckets = [bucket["Name"] for bucket in minio_client.list_buckets()["Buckets"]]

if bucket_name not in existing_buckets:
    minio_client.create_bucket(Bucket=bucket_name)
    print(f"Bucket '{bucket_name}' created successfully.")

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Spark Example MinIO") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.0,io.delta:delta-core_2.12:2.4.0") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", True) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

# Read data from Kafka topic
lines = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "postgres.public.customers") \
    .load()

# Convert Kafka value column to string
lines2 = lines.selectExpr("CAST(value AS STRING)")

# Define schema to extract `payload.after` data
json_schema = StructType([
    StructField("payload", StructType([
        StructField("after", table_schema),
        StructField("ts_ms", StringType()),
        StructField("op", StringType())
    ]))
])

# Parse JSON data from Kafka
parsedData = lines2.select(from_json(col("value"), json_schema).alias("data"))

# Extract only `after` data and filter for relevant operations
flattenedData = parsedData.select(
    col("data.payload.after.*"),
    col("data.payload.ts_ms"),
    col("data.payload.op")
).filter(col("op").isin(["c", "u"]))

# Drop `op` and `ts_ms` before writing to MinIO
flattenedData = flattenedData.drop("op", "ts_ms")

# Define MinIO storage paths
csv_output_path = "s3a://customers/csv"
parquet_output_path = "s3a://customers/parquet"
checkpoint_dir = "/tmp/spark-checkpoint"

# Write data to MinIO (CSV format) and print before writing
csvQuery = flattenedData \
    .writeStream \
    .format("csv") \
    .outputMode("append") \
    .option("path", csv_output_path) \
    .option("checkpointLocation", checkpoint_dir + "/csv") \
    .start()

# Write data to MinIO (Parquet format) and print before writing
parquetQuery = flattenedData \
    .writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", parquet_output_path) \
    .option("checkpointLocation", checkpoint_dir + "/parquet") \
    .start()

# Start Spark Streaming
csvQuery.awaitTermination()
parquetQuery.awaitTermination()
