package cn.edu.tsinghua

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import java.sql.Timestamp

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Quality {
  private val x=3
  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder()
//      .appName("Structured Streaming")
//      .master("local[2]")
//      .getOrCreate()
//
//    val userSchema = new StructType()
//      .add("time", "TimeStamp")
//      .add("CY35", "Integer")
//      .add("CY23", "Double")
//      .add("CY5", "Double")
//      .add("CY4", "Double")
//      .add("CY6", "Double")
//      .add("CY3", "Double")
//
//    val startTime = System.currentTimeMillis()
//
//    import spark.implicits._
//
//    val df = spark
//      .read
//      .format("csv")
//      .option("header", "true")
//      .option("timestampFormat","yyyy-MM-dd HH:mm:ss")
//      .schema(userSchema)
//      .load("src/main/data/test.csv")
//      .withColumn("timestamp", $"time".cast("long"))
//
//    //    val group = Window.partitionBy(window($"time", "5 minutes", "4 minutes")).orderBy("time")
//    val group = Window.orderBy("time")
//
//    val record = df
//      .withColumn("CY23Diff", ($"CY23" - lag("CY23", 1, 0).over(group)) / ($"timestamp" - lag("timestamp", 1, 1).over(group)))
//      //      .groupBy(window($"time", "5 minutes", "4 minutes"))
//      .agg(count($"CY23Diff").alias("CY23DiffCount"), avg($"CY23Diff").alias("CY23DiffAvg"), max($"CY23Diff").alias("CY23DiffMax"), min($"CY23Diff").alias("CY23DiffMin"))
//    //      .agg(count($"CY23").alias("CY23Count"), avg($"CY23").alias("CY23Avg"), max($"CY23").alias("CY23Max"), min($"CY23").alias("CY23Min"))
//
//    record.show(false)
//
//    val endTime = System.currentTimeMillis()
//    println("running time: "+(endTime - startTime))
//
//    record.explain(true)
    hi(4)
  }

  def hi(y: Int) = {
    var x = y
    println("in scala object")
    hello()
  }

  def hello() = {
    println(x)
  }

}