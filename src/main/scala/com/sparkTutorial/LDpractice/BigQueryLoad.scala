// spark-shell --jars=gs://hadoop-lib/bigquery/bigquery-connector-hadoop2-latest.jar
// spark-shell is started from gcp cluster master node

// part of the source code taken from here: https://stackoverflow.com/questions/32960707/dataproc-bigquery-examples-any-available

import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration
import com.google.cloud.hadoop.io.bigquery.BigQueryFileFormat
import com.google.cloud.hadoop.io.bigquery.GsonBigQueryInputFormat
import com.google.cloud.hadoop.io.bigquery.output.BigQueryOutputConfiguration
import com.google.cloud.hadoop.io.bigquery.output.IndirectBigQueryOutputFormat
import com.google.gson.JsonObject
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat

// Assumes you have a spark context (sc) -- running from spark-shell REPL.
// Marked as transient since configuration is not Serializable. This should
// only be necessary in spark-shell REPL.
@transient
val conf = sc.hadoopConfiguration

// Input parameters.
val fullyQualifiedInputTableId = "publicdata:samples.shakespeare"
val projectId = conf.get("fs.gs.project.id")
val bucket = conf.get("fs.gs.system.bucket")


//output setup
val fullyQualifiedOutputTableId = "mygcp1-240907:lddataset1.ld_table_1"
val outputTableSchema =
  "[{'name': 'id','type': 'INTEGER'},{'name': 'name','type': 'STRING'}]"
val jobName = "wordcount"

// Set the job-level projectId.
conf.set(BigQueryConfiguration.PROJECT_ID_KEY, projectId)

// Use the systemBucket for temporary BigQuery export data used by the InputFormat.
val systemBucket = conf.get("fs.gs.system.bucket")
conf.set(BigQueryConfiguration.GCS_BUCKET_KEY, systemBucket)

// Configure input and output for BigQuery access.
BigQueryConfiguration.configureBigQueryInput(conf, fullyQualifiedInputTableId)
BigQueryConfiguration.configureBigQueryOutput(conf,
  fullyQualifiedOutputTableId, outputTableSchema)

val fieldName = "word"

val tableData = sc.newAPIHadoopRDD(conf,
  classOf[GsonBigQueryInputFormat], classOf[LongWritable], classOf[JsonObject])
tableData.cache()
tableData.count()

tableData.map(entry => (entry._1.toString(),entry._2.toString())).take(10)

//https://tech.travelaudience.com/from-kafka-to-bigquery-with-spark-on-dataproc-in-scala-49c81756e291
//check this page for detailed analysis



