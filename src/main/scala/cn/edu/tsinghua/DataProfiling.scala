package cn.edu.tsinghua

import java.io.{BufferedWriter, FileWriter}

import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object DataProfiling {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Structured Streaming")
      .master("local[*]")
      .getOrCreate()

    val userSchema = new StructType()
      .add("time", "date")
      .add("CY35", "Integer")
      .add("CY23", "Double")
      .add("CY5", "Double")
      .add("CY4", "Double")
      .add("CY6", "Double")
      .add("CY3", "Double")

    val df = spark.readStream
      .option("seq", ",")
      .option("header", true)
      .option("includeTimestamp", true)
      .schema(userSchema)
      .csv("/Users/liuzhicheng/Desktop/git/SparkTest/src/main/resource")

    val time_col = "time"
    val time_col_as_long = time_col + "_asLong"


    val columns = df.schema.fields.filter(x => x.dataType == IntegerType || (x.dataType == DoubleType  && x.name != "CY6") || (x.dataType == LongType && x.name != time_col_as_long)).map(x => (x.name, "numeric")).array
    

    val result = df.agg(
      count("CY23"),
      avg("CY23"),
      stddev_pop("CY23"),
      max("CY23"),
      min("CY23"))

    val query = result.writeStream
      .outputMode("complete")
      .format("memory")
      .queryName("statistics")
       .trigger(Trigger.ProcessingTime(1000))
      .start()
    while(query.isActive){
      Thread.sleep(10000)
      val statistics = spark.sql("select * from statistics").first()
      val min_value = statistics.getDouble(4)
      val max_value = statistics.getDouble(3)
      val bin_number = 10
      val bin_width = (max_value - min_value) / bin_number * 1.000001

      val df_hist2 = df.withColumn("bin", floor((col("CY23") - min_value) / bin_width))
        .filter(col("bin").isNotNull)
        .groupBy("bin")
        .count()
        .orderBy("bin")

      import spark.implicits._

      val df_hist1 = Range(0, bin_number)
            .toList
            .map(i => (i, String.valueOf(i * bin_width + min_value) + "~" + String.valueOf((i + 1) * bin_width + min_value)))
            .toDF("bin", "xAxis")

      val df_hist = df_hist2.join(df_hist1, "bin")
        .withColumn("yAxis", when(col("count").isNotNull, col("count")).otherwise(0))
        .orderBy("bin")
        .drop("count")
        .drop("bin")
      df_hist.writeStream
        .outputMode("complete")
        .option("truncate", "false")
        .format("console")
        .start()

    }

    query.awaitTermination()

  }
}
