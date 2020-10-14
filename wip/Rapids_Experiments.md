# Testing Spark 3 with GPU and RAPIDS

## System setup:

Some ansible bits to get the spark 3 prepped

```{bash}

#downloading rapids jars
ansible -i aws_terraform/inventory extra_worker --private-key ../terraforming/secrets/brian_terra_key.pem --extra-vars "@secrets/secrets.yml" -m file -a "path=/opt/rapids state=directory" --become

ansible -i aws_terraform/inventory extra_worker --private-key ../terraforming/secrets/brian_terra_key.pem --extra-vars "@secrets/secrets.yml" -m get_url -a "url='https://repo1.maven.org/maven2/ai/rapids/cudf/0.15/cudf-0.15-cuda10-2.jar' dest='/opt/rapids/cudf-0.15-cuda10-2.jar' owner=cm_admin group=cdp_admins" --become

ansible -i aws_terraform/inventory extra_worker --private-key ../terraforming/secrets/brian_terra_key.pem --extra-vars "@secrets/secrets.yml" -m get_url -a "url='https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/0.2.0/rapids-4-spark_2.12-0.2.0.jar' dest='/opt/rapids/rapids-4-spark_2.12-0.2.0.jar' owner=cm_admin group=cdp_admins" --become

ansible -i aws_terraform/inventory extra_worker --private-key ../terraforming/secrets/brian_terra_key.pem --extra-vars "@secrets/secrets.yml" -m file -a "path='/opt/rapids/' owner=cm_admin group=cdp_admins mode='0755'" --become

ansible -i aws_terraform/inventory extra_worker --private-key ../terraforming/secrets/brian_terra_key.pem --extra-vars "@secrets/secrets.yml" -m copy -a "src='tmp/getGpusResources.sh' dest='/opt/rapids/getGpusResources.sh' owner=cm_admin group=cdp_admins mode='0755'" --become


```


# Running in Spark Shell



```{bash}

export SPARK_RAPIDS_DIR=/opt/rapids
export SPARK_CUDF_JAR=${SPARK_RAPIDS_DIR}/cudf-0.15-cuda10-2.jar
export SPARK_RAPIDS_PLUGIN_JAR=${SPARK_RAPIDS_DIR}/rapids-4-spark_2.12-0.2.0.jar

# kinit first

spark3-shell \
  --conf spark.rapids.sql.concurrentGpuTasks=1 \
  --driver-memory 2G \
  --conf spark.executor.memory=2G \
  --conf spark.executor.cores=2 \
  --conf spark.executor.resource.gpu.amount=1 \
  --conf spark.task.cpus=2 \
  --conf spark.task.resource.gpu.amount=1 \
  --conf spark.rapids.memory.pinnedPool.size=2G \
  --conf spark.locality.wait=0s \
  --conf spark.sql.files.maxPartitionBytes=512m \
  --conf spark.sql.shuffle.partitions=10 \
  --conf spark.kryo.registrator=com.nvidia.spark.rapids.GpuKryoRegistrator \
  --conf spark.plugins=com.nvidia.spark.SQLPlugin \
  --conf spark.rapids.shims-provider-override=com.nvidia.spark.rapids.shims.spark301.SparkShimServiceProvider \
  --conf spark.executor.resource.gpu.discoveryScript=${SPARK_RAPIDS_DIR}/getGpusResources.sh \
  --jars  ${SPARK_CUDF_JAR},${SPARK_RAPIDS_PLUGIN_JAR}

# rebuilding the spark command
# take 2 to avoid errors
spark3-shell \
  --master yarn \
  --deploy-mode client \
  --driver-cores 6 \
  --driver-memory 15G \
  --executor-cores 8 \
  --conf spark.executor.memory=15G \
  --conf spark.rapids.sql.concurrentGpuTasks=4 \
  --conf spark.executor.resource.gpu.amount=1 \
  --conf spark.rapids.sql.enabled=true \
  --conf spark.rapids.sql.explain=ALL \
  --conf spark.rapids.memory.pinnedPool.size=2G \
  --conf spark.kryo.registrator=com.nvidia.spark.rapids.GpuKryoRegistrator \
  --conf spark.plugins=com.nvidia.spark.SQLPlugin \
  --conf spark.rapids.shims-provider-override=com.nvidia.spark.rapids.shims.spark301.SparkShimServiceProvider \
  --conf spark.executor.resource.gpu.discoveryScript=${SPARK_RAPIDS_DIR}/getGpusResources.sh \
  --jars  ${SPARK_CUDF_JAR},${SPARK_RAPIDS_PLUGIN_JAR}



```

Extra rapids flags to set
to set during running

add in spark.conf.set("<flag>", "var")

Enabling rapids SQL and explains so that we know when things are going to rapids and when they arent

--conf spark.rapids.sql.enabled=true
--conf spark.rapids.sql.explain=ALL

Pinning memory helps with throughput
But is detrimental to full system 
--conf spark.rapids.memory.pinnedPool.size=2G \
  
Increases parallelism too high will blow up GPU mem
Recommendation is 2 - tune by job
spark.conf.get("spark.rapids.sql.concurrentGpuTasks")

spark.conf.set("spark.rapids.sql.concurrentGpuTasks", "4")
--conf spark.rapids.sql.concurrentGpuTasks=2 \

spark.conf.set("spark.rapids.sql.castFloatToString.enabled", "true")
--conf spark.rapids.sql.castFloatToString.enabled true