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

#### spark submit
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

