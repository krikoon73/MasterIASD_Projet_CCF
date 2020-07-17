command : 
```
spark-submit --master yarn \
--deploy-mode cluster \
--executor-cores 4 \
--num-executors 11 \
--executor-memory 5g \
--conf spark.executor.memoryOverhead=2g \
--conf spark.driver.memory=5g \
--conf spark.driver.cores=1 \
--conf spark.yarn.jars="file:///home/cluster/shared/vms/spark-current/jars/*.jar" \
kmeans-dario-x.py
```
