package cn.edu.tsinghua

import org.apache.spark.sql.functions.{avg, count, max, min, stddev_pop}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.streaming.Trigger

class Profiling(bucketNum: Int, kSigma: Int) {
  private val bin_number = bucketNum
  private val k_sigma = kSigma

  def calculateIndex(spark: SparkSession, df: DataFrame, col: String, filePath: String) : Unit = {
    val statistcs = df.agg(
      count(col),
      avg(col),
      stddev_pop(col),
      max(col),
      min(col))

    val query = statistcs.writeStream
        .outputMode("complete")  // append模式不能用于聚合操作
      .format("parquet")  // 不需要outputmode, 需要指定检查点
      .option("path", filePath)
      .trigger(Trigger.ProcessingTime("25 seconds"))
      .option("checkpointLocation", filePath)
      .start()
    query.awaitTermination()
  }

}


object Process{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Data Profiling")
      .master("local[*]")
      .getOrCreate()

    val schema = new StructType()
      .add("time", "Timestamp")
      .add("CY35", "Integer")
      .add("CY23", "Double")
      .add("CY5", "Double")
      .add("CY4", "Double")
      .add("CY6", "Double")
      .add("CY3", "Double")

    val filePath = "/Users/liuzhicheng/Desktop/git/SparkTest/src/main/resource"

    val df = spark.readStream
      .option("header", "true")
      .schema(schema)
      .csv(filePath)

    val profiling = new Profiling(10, 3)
    profiling.calculateIndex(spark, df, "CY23", "/Users/liuzhicheng/Desktop/git/SparkTest/src/main/output")
//    val query = df.writeStream
//      .outputMode("update")
//      .format("console")
//      .option("truncate", "false")
//      .trigger(Trigger.ProcessingTime("2 seconds"))
//      .start()
//    query.awaitTermination()
  }
}