import findspark

from pyspark.sql import SparkSession

# Cassandra
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = (
    '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,'
    'com.datastax.spark:spark-cassandra-connector_2.11:2.4.1 '
    '--conf spark.cassandra.connection.host=localhost '
    'pyspark-shell'
)

findspark.init()

if __name__ == "__main__":
    def handle_rdd(rdd):
        if not rdd.isEmpty():
            global cassandraSparkSession
            dataframe = cassandraSparkSession.createDataFrame(rdd, schema=['word', 'count'])
            dataframe.show(5)
            dataframe.write \
                .format("org.apache.spark.sql.cassandra") \
                .mode('append') \
                .options(table="words", keyspace="streaming_test") \
                .save()


    cassandraSparkSession = SparkSession.builder \
        .appName('SparkCassandraApp') \
        .config('spark.cassandra.connection.host', 'localhost') \
        .config('spark.cassandra.connection.port', '9042') \
        .config('spark.cassandra.output.consistency.level', 'ONE') \
        .getOrCreate()

    kafkaSparkSession = SparkSession.builder.getOrCreate()

    message = kafkaSparkSession.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "trafficSystem") \
        .option("startingOffsets", "earliest") \
        .load()

    # lines = message.map(lambda x: x[1])
    # transform = lines.map(lambda tweet: (tweet, int(len(tweet.split()))))
    # transform.foreachRDD(handle_rdd)
    # Write stream - console
    query = message.selectExpr("CAST(value AS STRING)") \
        .writeStream \
        .format("console") \
        .option("truncate", "false") \
        .start()

    query.awaitTermination()

