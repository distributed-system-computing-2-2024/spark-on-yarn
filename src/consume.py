import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, StringType

findspark.init()

if __name__ == "__main__":
    # SparkSession 생성
    spark = SparkSession.builder \
        .appName('SparkKafkaCassandraApp') \
        .config("spark.cassandra.connection.host", "cassandra1") \
        .config("spark.cassandra.connection.port", "9042") \
        .getOrCreate()

    # Kafka 메시지 JSON 스키마 정의
    schema = StructType([
        StructField("stdHour", StringType(), True),
        StructField("routeNo", StringType(), True),
        StructField("routeName", StringType(), True),
        StructField("updownTypeCode", StringType(), True),
        StructField("vdsId", StringType(), True),
        StructField("trafficAmout", StringType(), True),
        StructField("shareRatio", StringType(), True),
        StructField("conzoneId", StringType(), True),
        StructField("conzoneName", StringType(), True),
        StructField("stdDate", StringType(), True),
        StructField("speed", StringType(), True),
        StructField("timeAvg", StringType(), True),
        StructField("grade", StringType(), True),
    ])

    # Kafka에서 스트리밍 데이터 읽기
    kafka_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "trafficSystem") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()


    # JSON 형식의 value를 파싱하여 데이터 프레임으로 변환
    df = kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    parsed_df = df.withColumn("Body", from_json(col("value"), schema)).select(col("Body.*"))

    # 콘솔 결과 출력 (이부분은 발표자료 필요없음)
    query = parsed_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    # Cassandra 스키마에 맞춰 변환
    transformed_df = parsed_df.select(
        col("vdsId").alias("road_id"),
        col("conzoneName").alias("road_name"),
        col("stdDate").alias("std_date"),
        col("stdHour").alias("std_hour"),
        expr("CASE "
             "WHEN CAST(speed AS INT) >= 80 THEN 'low' "
             "WHEN CAST(speed AS INT) BETWEEN 30 AND 79 THEN 'middle' "
             "ELSE 'high' "
             "END").alias("congestion_level")
    )

    # Cassandra 배치 작업 함수
    def foreach_batch_function(batch_df, batch_id):
        batch_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="road", keyspace="dsc2024") \
            .mode("append") \
            .save()


    # Cassandra에 데이터 저장
    query2 = transformed_df.writeStream \
        .foreachBatch(foreach_batch_function) \
        .option("checkpointLocation", "/tmp/checkpoints") \
        .start()

    query.awaitTermination()
    query2.awaitTermination()
