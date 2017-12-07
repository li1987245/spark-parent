spark-submit  --class CalculateIdCard  --master yarn --deploy-mode cluster --conf spark.default.parallelism=1000 --driver-memory 4g   --executor-memory 4g --executor-cores 2  --num-executors 35   *.jar

spark-shell --master yarn --conf spark.default.parallelism=1200 --driver-memory 8g   --executor-memory 8g