## submit job
```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,org.apache.hadoop:hadoop-aws:3.4.2 \
  /opt/spark/work/bronze_job.py
```
or
```bash
docker exec airflow /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,org.apache.hadoop:hadoop-aws:3.4.2 \
  /opt/project/code/spark/bronze_stream_job.py
```
