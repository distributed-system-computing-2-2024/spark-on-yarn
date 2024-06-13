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
spark-submit --master yarn --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.8 src/consume.py
```

