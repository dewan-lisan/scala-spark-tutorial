//spark-shell --jars $INFA_SHARED/common/libs/spark-avro_2.11-4.0.0.jar --conf spark.hadoop.avro.mapred.ignore.inputs.without.extension=false

val df = spark.read.format("com.databricks.spark.avro").load("/raw/loan/lln/NTR/2019/06/03/LANT200.snz")
df.createOrReplaceTempView("source")

spark.sql("select reg_timestamp from source").show()