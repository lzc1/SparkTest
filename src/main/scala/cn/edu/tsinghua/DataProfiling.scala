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

//    columns.foreach(c => df.agg(
//      count(c._1).alias("count"),
//      avg(c._1).alias("avg"),
//      stddev_pop(c._1).alias("std"),
//      max(c._1).alias("max"),
//      min(c._1).alias("min")).withColumn("Feature", lit(c._1)).writeStream
//      .outputMode("complete")
//      .format("console")
//      .option("truncate", "false")
//      .start())

    val result = df.agg(
      count("CY23"),
      avg("CY23"),
      stddev_pop("CY23"),
      max("CY23"),
      min("CY23"))

//    var a = 3
//    import spark.implicits._
//    val test = result.map(row=>(row.getDouble(3)-row.getDouble(4))/10)
////    val df_hist = test.map(value=>df.withColumn("var", col("CY23")/value))
//
//    val hist_columns = test.schema.fields.filter(x => x.dataType == IntegerType || (x.dataType == DoubleType  && x.name != "CY6") || (x.dataType == LongType && x.name != time_col_as_long)).map(x => (x.name, "numeric")).array
////    hist_columns.foreach(c=>df.withColumn("group", col("CY23")/test.col(c._1)).writeStream
////            .outputMode("update")
////            .format("console")
////            .option("truncate", "false")
////            .start())
//
//    result.writeStream
//      .outputMode("complete")
//      .foreach(new ForeachWriter[Row] {
//
//        def open(partitionId: Long, version: Long): Boolean = {
//          true
//        }
//
//        def process(value: Row): Unit = {
////          df.writeStream
////            .outputMode("update")
////            .format("console")
////            .option("truncate", "false")
////            .start()
//        }
//
//        def close(errorOrNull: Throwable): Unit = {
//
//        }
//
//      }).start()
//
//    println(a)
//    result.writeStream
//        .outputMode("complete")
//        .foreach(new ForeachWriter[Row] {
//          var fw: FileWriter = null
//          var bw: BufferedWriter = null
//          def open(partitionId: Long, version: Long): Boolean = {
//            try{
//              fw = new FileWriter("/Users/liuzhicheng/Desktop/git/SparkTest/src/test/resource/result.txt")
//              bw = new BufferedWriter(fw)
//              true
//            }catch {
//              case e:Exception=>false
//            }
//          }
//
//          def process(value: Row): Unit = {
//            bw.write(value.toString())
//            println(value.toString())
//            bw.flush()
//          }
//
//          def close(errorOrNull: Throwable): Unit = {
////            fw.close()
////            bw.close()
//          }
//        }).start()

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
//            .join(df_hist2, usingColumns = Seq("bin"), joinType = "left")
//            .withColumn("yAxis", when(col("count").isNotNull, col("count")).otherwise(0))
//            .orderBy("bin")
//            .drop("count")
//            .drop("bin")
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
////            .withColumn("Attribute", lit(attribute))
////            .withColumn("FeatureType", lit(featureType))
////            .withColumn("TimeAttr", lit(time))
    }

//    val window = Window.orderBy(time_col)
//    val df_new = df.withColumn(time_col_as_long, col(time_col).cast("long"))
//    val df_final = df_new.withColumn("variation", col("CY23") - lag(col("CY23"), 1, 0).over(window))
//      .withColumn("interval", col(time_col_as_long) - lag(time_col_as_long, 1, 0).over(window))
//      .withColumn("speed", col("variation") / col("interval"))
//      .withColumn("acceleration", col("speed") - lag("speed", 1, 0).over(window))

//  val result = df_final.agg(
//          count("acceleration"),
//          avg("acceleration"),
//          stddev_pop("acceleration"),
//          max("acceleration"),
//          min("acceleration"))
//    val query = df_final.writeStream
//    .format("console")
//    .outputMode("append")
//    .trigger(Trigger.Once())
//    .start()
    query.awaitTermination()

//    val spark = SparkSession.builder()
//          .appName("Structured Streaming")
//          .master("local[*]")
//          .getOrCreate()
//
//    val df = spark.read
//      .format("csv")
//      .option("header", "true")
//      .option("inferschema", "true")
//      .load("/Users/liuzhicheng/Desktop/git/SparkTest/src/main/resource/1701_2019-01_sample.csv")
//
//    val result = df.agg(
//          count("CY23"),
//          avg("CY23"),
//          stddev_pop("CY23"),
//          max("CY23"),
//          min("CY23"))
//    result.show()
  }
}
