from pyspark.sql import SparkSession, DataFrame
from config import configuration
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType


def main():
    spark = SparkSession.builder.appName("RushHourUpdates") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,"
                "org.apache.hadoop:hadoop-aws:3.3.1,"
                "com.amazonaws:aws-java-sdk:1.11.469") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", configuration.get('AWS_ACCESS_KEY')) \
        .config("spark.hadoop.fs.s3a.secret.key", configuration.get('AWS_SECRET_KEY')) \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

    # Adjust the log level to minimize the console output on executors
    spark.sparkContext.setLogLevel("WARN")

    # Vehicle schema
    vehicle_schema = StructType([
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("fuel_type", StringType(), True)
    ])

    # GPS schema
    gps_schema = StructType([
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("vehicle_type", StringType(), True)
    ])

    # Traffic schema
    traffic_schema = StructType([
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("camera_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("snapshot", StringType(), True),
    ])

    # Weather schema
    weather_schema = StructType([
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("weather_condition", StringType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("wind_speed", DoubleType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("air_quality_index", DoubleType(), True)
    ])

    # Emergency schema
    emergency_schema = StructType([
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("incident_id", StringType(), True),
        StructField("type", StringType(), True),
        StructField("snapshot", StringType(), True),
        StructField("status", StringType(), True),
        StructField("description", StringType(), True)
    ])

    def read_kafka_topic(topic, schema):
        return (spark.readStream
                .format('kafka')
                .option('kafka.bootstrap.servers', 'broker:29092')
                .option('subscribe', topic)
                .option('starting_offsets', 'earliest')
                .load()
                .selectExpr('CAST(value AS STRING)')
                .select(from_json(col('value'), schema).alias('data'))
                .select('data.*')
                .withWatermark('timestamp', '2 minutes')
                )

    def stream_writer(input_parameter: DataFrame, checkpoint_folder, output):
        return (input_parameter.writeStream
                .format('parquet')
                .option('checkpoint_location', checkpoint_folder)
                .option('path', output)
                .outputMode('append')
                .start()
                )

    # Read data
    vehicle_df = read_kafka_topic('vehicle_data', vehicle_schema).alias('vehicle')
    gps_df = read_kafka_topic('gps_data', gps_schema).alias('gps')
    traffic_df = read_kafka_topic('traffic_data', traffic_schema).alias('traffic')
    weather_df = read_kafka_topic('weather_data', weather_schema).alias('weather')
    emergency_df = read_kafka_topic('emergency_data', emergency_schema).alias('emergency')

    query_1 = stream_writer(vehicle_df, 's3a://spark-streaming-data/checkpoints/vehicle_data',
                            's3a://spark-streaming-data/data/vehicle_data')
    query_2 = stream_writer(gps_df, 's3a://spark-streaming-data/checkpoints/gps_data',
                            's3a://spark-streaming-data/data/gps_data')
    query_3 = stream_writer(traffic_df, 's3a://spark-streaming-data/checkpoints/traffic_data',
                            's3a://spark-streaming-data/data/traffic_data')
    query_4 = stream_writer(weather_df, 's3a://spark-streaming-data/checkpoints/weather_data',
                            's3a://spark-streaming-data/data/weather_data')
    query_5 = stream_writer(emergency_df, 's3a://spark-streaming-data/checkpoints/emergency_data',
                            's3a://spark-streaming-data/data/emergency_data')

    query_5.awaitTermination()


if __name__ == "__main__":
    main()
