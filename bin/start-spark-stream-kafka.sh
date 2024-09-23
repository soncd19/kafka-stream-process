#!/bin/bash
user_dir="$(cd -P -- "$(dirname -- "$0")" && pwd -P)"
echo ${user_dir}

/data/spark-3.2.3-bin-hadoop3.2/bin/spark-submit \
  --name "com.msb.stream.application.EDTStreamFilterProcess" \
  --master spark://10.0.65.157:7077 \
  --conf "spark.cores.max=3" \
  --conf "spark.executor.instances=3" \
  --conf "spark.executor.cores=3" \
  --conf "spark.dynamicAllocation.minExecutors=1" \
  --conf "spark.dynamicAllocation.maxExecutors=3" \
  --deploy-mode client \
  --total-executor-cores 3 \
  --executor-cores 1 \
  --executor-memory 1G \
  --driver-memory 1G \
  --class com.msb.stream.App \
  ${user_dir}/msb-edt-streaming-process-1.0.jar \
  "KafkaStreamingProcess" "${user_dir}/config/application.properties"

#--master spark://10.0.65.157:7077,10.0.65.158:7077
