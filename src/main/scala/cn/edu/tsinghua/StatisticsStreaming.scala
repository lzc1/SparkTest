package cn.edu.tsinghua

import java.io.{File, FileOutputStream, FileWriter}

import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SQLContext, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object StatisticsStreaming {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Structured Streaming")
      .master("local[*]")
      .getOrCreate()

//    import spark.sqlContext.implicits._
//    val site: List[String] = List("hello", "world")
//              site.toDF().show()  // 在这里面不能用

    // test structured streaming
    val userSchema = new StructType()
      .add("time", "Timestamp")
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

    // expand
    //    val name = "CY35"
    //    val speed_suffix = "-speed"
    //    val acceleration_suffix = "-accelerated"
    //    val variation_suffix = "-variation"
    //    val time_suffix = "_asLong"
    //    val interval_suffix = "-interval"
    //    val interval_name = name + interval_suffix
    //    val variation_name = name + variation_suffix
    //    val speed_name = name + speed_suffix
    //    val acceleration_name = name + acceleration_suffix
    //    val group = Window.orderBy(time_col)
    //
    //    val expand_df = df.withColumn(time_col_as_long, col(time_col).cast("long"))
    //
    //    expand_df.withColumn(variation_name, col(name) - lag(col(name), 1, 0).over(group))
    //      .withColumn(interval_name, col(time_col_as_long) - lag(time_col_as_long, 1, 0).over(group))
    //      .withColumn(speed_name, col(variation_name) / col(interval_name))
    //      .withColumn(acceleration_name, col(speed_name) - lag(speed_name, 1, 0).over(group))


    val statistics = columns.map(c => df.agg(
      count(c._1).alias("count"),
      avg(c._1).alias("avg"),
      stddev_pop(c._1).alias("std"),
      max(c._1).alias("max"),
      min(c._1).alias("min")).withColumn("Feature", lit(c._1))
      .writeStream
      .outputMode("complete")
      .foreach(new ForeachWriter[Row] {
        var fWriter: FileWriter = null
        val outputfile = "/Users/liuzhicheng/Desktop/git/SparkTest/src/main/output/result.txt"
        override def open(partitionId: Long, version: Long): Boolean = {
          try{
            fWriter = new FileWriter(outputfile, false)
            true
          }
          catch{
            case e:Exception =>false
          }
        }

        override def process(value: Row): Unit = { // 可以获取值
          fWriter.write(value.toString() + "\n") // 写入文件可能冲突
          println(value.toString())
        }

        override def close(errorOrNull: Throwable): Unit = {
          fWriter.close()
        }
      }).start())

    //    statistics.foreach(new ForeachWriter[Row] {
    //
    //    })
//    val combine = new combine
    columns.foreach(c => df.agg(
      count(c._1).alias("count"),
      avg(c._1).alias("avg"),
      stddev_pop(c._1).alias("std"),
      max(c._1).alias("max"),
      min(c._1).alias("min")).withColumn("Feature", lit(c._1)).writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .start())

    //    val statistics = df.groupBy("CY35").count()
    val result = df.agg(
      count("CY6"),
      avg("CY6"),
      stddev_pop("CY6"),
      max("CY6"),
      min("CY6"))
//    var num = 1L
//    result.writeStream
//      .outputMode("complete")
//      .foreach(new ForeachWriter[Row] {
//        var fos:FileOutputStream=null
//        val outputfile = "/Users/liuzhicheng/Desktop/git/SparkTest/src/main/output/result.txt"
//        override def open(partitionId: Long, version: Long): Boolean = {
//          try{
//            fos=new FileOutputStream(outputfile);
//            true
//          }
//          catch{
//            case e:Exception =>false
//          }
//        }
//
//        override def process(value: Row): Unit = { // 可以获取值
//          fos.write(value.mkString.getBytes)
//        }
//
//        override def close(errorOrNull: Throwable): Unit = {
//          fos.close()
//        }
//      }).start()
//      println("count: " + num)  // 事实上，这个先于前面执行了，因为前面必须使用start以后才开始，懒计算
    //    val result = statistics.union(statistics

    val query = result.writeStream
      .outputMode("update")
      .format("console")
      .option("truncate", "false")
      .start()
    query.awaitTermination()
  }
}
class combine extends Serializable {
  private var result: Map[String, Row] = Map()
  def combineStreaming(spark: SparkSession, streamingDF: DataFrame): Unit = {
    streamingDF.writeStream
      .outputMode("complete")
      .foreach(new ForeachWriter[Row] {
        import spark.sqlContext.implicits._
        var fos:FileOutputStream=null
        val outputfile = "/Users/liuzhicheng/Desktop/git/SparkTest/src/main/output"
        override def open(partitionId: Long, version: Long): Boolean = {
          try{
            fos=new FileOutputStream(outputfile);
            true
          }
          catch{
            case e:Exception =>false
          }
        }

        override def process(value: Row): Unit = {
//            result += (value.getString(5)->value)
//          Seq(value.toSeq).toDF().show()
//          val site: List[String] = List("hello", "world")
//          site.toDF().show()  // 在这里面不能用
//          println(result.size)
          fos.write(value.mkString.getBytes)
        }

        override def close(errorOrNull: Throwable): Unit = {
          fos.close
        }
      }).start()
  }

}
