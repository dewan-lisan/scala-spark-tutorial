//spark-shell --jars=gs://hadoop-lib/bigquery/bigquery-connector-hadoop2-latest.jar --packages com.spotify:spark-bigquery_2.10:0.2.0

//--packages com.spotify:spark-bigquery_2.10:0.2.0  THIS PACKAGE DOESNT WORK FOR DF TO BIGQUERY
//com.miraisolutions.spark.bigquery.DefaultSource
//--packages com.spotify:spark-bigquery_2.11:0.2.5

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import com.samelamin.spark.bigquery._


//NB: all the option should be in one line for spark-shell
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration
import com.google.cloud.hadoop.io.bigquery.BigQueryFileFormat
import com.google.cloud.hadoop.io.bigquery.GsonBigQueryInputFormat
import com.google.cloud.hadoop.io.bigquery.output.BigQueryOutputConfiguration
import com.google.cloud.hadoop.io.bigquery.output.IndirectBigQueryOutputFormat
import com.google.gson.JsonObject
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat

import com.spotify.spark.bigquery._


// Assumes you have a spark context (sc) -- running from spark-shell REPL.
// Marked as transient since configuration is not Serializable. This should
// only be necessary in spark-shell REPL.
@transient
val conf = sc.hadoopConfiguration

val df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("gs://x-sampledata/creditcard.csv")  //csv file from x-sampledata bucket
df.createOrReplaceTempView("creditcard_v")
df.count()


// Input parameters.
val projectId = conf.get("fs.gs.project.id")   //projectId: String = helloworlddataproc-224810
val bucket = conf.get("fs.gs.system.bucket")    //bucket: String = ld_ml_bucket
//val fullyQualifiedInputTableId = "publicdata:samples.shakespeare" //not used in my case
val fullyQualifiedInputTableId = s"${projectId}:${bucket}.carddata.csv"

// Input configuration.
conf.set(BigQueryConfiguration.PROJECT_ID_KEY, projectId)
conf.set(BigQueryConfiguration.GCS_BUCKET_KEY, bucket)
BigQueryConfiguration.configureBigQueryInput(conf, fullyQualifiedInputTableId)

// Output parameters.
//val outputTableId = projectId + ":wordcount_dataset.wordcount_output"
//// Temp output bucket that is deleted upon completion of job.
//val outputGcsPath = ("gs://" + bucket + "/hadoop/tmp/bigquery/wordcountoutput")


//BIG QUERY DATASET CREATED LIKE THIS: bq mk bq_carddata_dataset
//df.saveAsBigQueryTable(s"${projectId}:bq_carddata_dataset.carddata_v1")
val outputTableId = projectId + ":bq_carddata_dataset.carddata_tbl"
val outputGcsPath = ("gs://" + bucket + "/hadoop/tmp/bigquery/carddata_tbl_output")

df.saveAsBigQueryTable(outputTableId)


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

// Input configuration.
conf.set(BigQueryConfiguration.PROJECT_ID_KEY, projectId)
conf.set(BigQueryConfiguration.GCS_BUCKET_KEY, bucket)
BigQueryConfiguration.configureBigQueryInput(conf, fullyQualifiedInputTableId)

// Output parameters.
val outputTableId = projectId + ":bq_carddata_dataset.carddata_tbl"
// Temp output bucket that is deleted upon completion of job.
val outputGcsPath = ("gs://" + bucket + "/hadoop/tmp/bigquery/carddata_query")

// Write data back into a new BigQuery table.
// IndirectBigQueryOutputFormat discards keys, so set key to null.
  .map(pair => (null, convertToJson(pair)))
(df.saveAsNewAPIHadoopDataset(conf))




