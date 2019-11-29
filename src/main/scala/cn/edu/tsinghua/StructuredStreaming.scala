package cn.edu.tsinghua

import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import java.sql.Timestamp
import java.util

import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions



object StructuredStreaming {

  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()
    val spark = SparkSession.builder()
      .appName("Structured Streaming")
      .master("local")
      .getOrCreate()
    import spark.sqlContext.implicits._
    //          Seq(value.toSeq).toDF().show()
    val site: List[String] = List("hello", "world")
    site.toDF().show()

    var a: Map[String, String] = Map()
    a += ("1"->"2")

//    val userSchema = new StructType()
//          .add("time", "String")
//          .add("CY35", "Integer")
//          .add("CY23", "Double")
//          .add("CY5", "Double")
//          .add("CY4", "Double")
//          .add("CY6", "Double")
//          .add("CY3", "Double")
//
//    val df = spark.readStream
//      .option("seq", ",")
//      .option("header", true)
////          .option("inferschema", true)
//      .schema(userSchema)
//      .csv("/Users/liuzhicheng/Desktop/git/SparkTest/src/main/resource")

    import spark.implicits._
//    val windowedCounts = df.groupBy(
//      window($"time", "5 minutes", "4 minutes"), $"CY35")
//      .count().orderBy("window")
//        val windowedCounts = df.groupBy(
//              window($"CY23", "10", "8"), $"CY35")
//              .count().orderBy("window")

//        df.select("time")
//          .writeStream
//          .queryName("time_table")
//          .outputMode("complete")
//          .format("memory").start()
//
//        spark.sql("select * from time_table").show()

//    val result = df.agg(count("CY35"))
//    result.writeStream
//      .outputMode("complete")
//      .format("console")
//      .option("truncate", "false")
//      .start()
//    val row = result.select("count(CY35)")
//    val str = result.col("count(CY35)")
////    println(str)
//    result.writeStream
//          .queryName("time_table")
//          .outputMode("complete")
//          .format("memory").start()
//    val table = spark.sql("select * from time_table")

//    val row1 = table.first()
//    println(row1.get(0))
//    val query = row.writeStream
//      .outputMode("complete")
//      .format("console")
//      .option("truncate", "false")
//      .start()
//
//    query.awaitTermination()
    import spark.implicits._
//    val df = spark.read
//      .format("csv")
//      .option("header", "true")
//      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss.SSS")
//      .option("inferschema", "true")
//      .load("/Users/liuzhicheng/Desktop/git/SparkTest/src/test/resource/test.csv")
//    val columns = df.schema.fields.filter(x=>x.dataType == IntegerType || x.dataType == DoubleType || x.dataType == LongType).map(x=>x.name)
////    for(a <- columns){
////      println(a)
////    }
//    println(columns.size)
//    columns.map(x=>df.select(x.name)).reduce((a,b)=>a.union(b)).show()
//    df.schema.fields.foreach(x=>println(x.dataType))
//    val names = columns.map(x=>println(x.name))
//    println(columns.size)
//    names.foreach(x=>println(x))

//    df.filter($"value"<3 || $"value">5).show()
////    df.agg(avg("value"))
//    df.filter($"value"<3).show()
//    df.agg(avg("value").alias("mean"), stddev("value").alias("std")).show()
//    val endTime = System.currentTimeMillis()
//    println("程序运行时长为："+(endTime - startTime))
//    val lines = spark.readStream
//      .format("socket")
//      .option("host", "localhost")
//      .option("port", 9999)
//      .load()
//
//    import spark.implicits._
//    val words = lines.as[String].flatMap(_.split(" "))
//    val wordCounts = words.groupBy("value").count()
//    val query = wordCounts.writeStream
//      .outputMode("Update")
//      .format("console")
//      .start()
//    query.awaitTermination()
//    val query = wordCounts.writeStream
//          .queryName("time_table")
//          .outputMode("complete")
//          .format("memory").start()
//
//    query.awaitTermination()
//    val table = spark.sql("select * from time_table")
//    val first = table.first();
//    println(first.get(0))

  }

//  def main(args: Array[String]): Unit = {
//    val startTime = System.currentTimeMillis()
//    val spark = SparkSession.builder()
//      .appName("Structured Streaming")
//      .master("local")
//      .getOrCreate()
//
//    val df = spark.read
//      .format("csv")
//      .option("header", "true")
//      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss.SSS")
//      .option("inferschema", "true")
//      .load("/Users/liuzhicheng/Desktop/git/SparkTest/src/test/resource/empty.csv")
//    val columns = df.schema.fields.filter(x=>x.dataType == IntegerType || x.dataType == DoubleType || x.dataType == LongType).map(x=>x.name)
//
//    val statistics = columns.map(x=>df.agg(
//      count(x).alias("count"),
//      avg(x).alias("mean"),
//      stddev_pop(x).alias("std"),
//      max(x).alias("max"),
//      min(x).alias("min")
//    ).withColumn("zero", lit(df.filter(df(x) === 0.0).count())).withColumn("name", lit(x.toString))).reduce((a, b) => a.union(b))
//
////    statistics.show()
//
//    import spark.implicits._
////    df.createOrReplaceGlobalTempView("resultTable")  // 创建全局视图，需要在使用到这个视图的时候加前缀global_temp.
//    val timeCol = "time"
////    val newDf = df.orderBy(timeCol).withColumn("rowNum", monotonically_increasing_id())
//    val newDf = df.withColumn("timeStamp", $"time".cast("long"))
////    newDf.show()
//    newDf.createOrReplaceTempView("resultTable")
////    newDf.createOrReplaceGlobalTempView("resultTable")
//
////    statistics.map(x=>x.getAs[Double](1) + x.getAs[Double](2)).show()
////    statistics.map(x=>spark.sql("select " + x.getAs[String]("name") + "from table where " +
////      x.getAs[String]("name") + "<" + x.getAs[Double]("mean"))).reduce((a,b)=>a.union(b)).show()
////    statistics.rdd.map(x=>df.filter($"" + x.get(6) < x.get(1))).reduce((a,b)=>a.union(b)).show()  // 如果union的过程中出现了空的dataset，就会报空指针异常
//
//    val statisticsTable = statistics.collect()
////    var result = spark.emptyDataFrame
//    val schema = StructType(List(
////      StructField("rowNum", StringType, true),
//      StructField("Attribute", StringType, true),
//      StructField("OutlierID", StringType, true),
//      StructField("NeighborID", StringType, true),
//      StructField("Value", DoubleType, true)
//    ))
//    var result = spark.createDataFrame(new util.ArrayList[Row](), schema)
//    for(i <- 0 to statisticsTable.length -1 ){
//      val mean = statisticsTable(i).getAs[Double]("mean")
//      val std = statisticsTable(i).getAs[Double]("std")
//      val col = statisticsTable(i).getAs[String]("name")
//      val lower = mean - 2*std
//      val upper = mean + 2*std
////      val sql = "(select " + timeCol + " as OutlierID,rowNum from resultTable where " + col + "<" + lower + " or " + col +
////        ">" + upper + ") table1"
//      val sql = "(select " + timeCol + " as OutlierID, timeStamp as timeStamp1 from resultTable where " + col + "<" + lower + " or " + col +
//        ">" + upper + ") table1"
////      spark.sql(sql).show()  // bug?
//      val sql1 = "select \'" + col + "\' as Attribute,OutlierID," + timeCol + " as NeighborID," + col +
//        " as Value from resultTable, "+ sql + " where abs(timeStamp - timeStamp1) < 30"
//      val temp = spark.sql(sql1)
//////      temp.show()
//////      if (result.count() == 0){
//////        result = temp
//////      }else if (temp.count != 0){
//////        result = result.union(temp)
//////      }
//      result = result.union(temp)
//    }
//    result.show()
//    val endTime = System.currentTimeMillis()
//    println("程序运行时长为："+(endTime - startTime))
//  }


}
