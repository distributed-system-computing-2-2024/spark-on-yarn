## Usage 
### Build 
```bash
make build
```
### Run 
```bash
make start
```
### Stop
```bash
make stop
```
### Connect to Master Node
```bash
make connect
```
```bash
 ---- MASTER NODE ---- 
root@cluster-master:/#
```
### Run spark applications on cluster : 
Once connected to the master node
 
#### spark-shell
```bash 
spark-shell --master yarn --deploy-mode client
```
#### spark submit 
```bash
spark-submit --master yarn --deploy-mode client --num-executors 2 --executor-memory 4G --executor-cores 4 --class org.apache.spark.examples.SparkPi $SPARK_HOME/examples/jars/spark-examples_2.11-2.4.1.jar
```
```bash
spark-submit --master yarn --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.8,com.datastax.spark:spark-cassandra-connector_2.11:2.4.1 \
    --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:/src/log4j.properties -Dfile.encoding=UTF-8" \
    --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:/src/log4j.properties -Dfile.encoding=UTF-8" \
    --files "/src/log4j.properties" \
    src/consume.py
```
create table road
(
    road_id          text primary key,
    congestion_level text,
    road_name        text,
    std_date        text,
    std_hour        text,
);

