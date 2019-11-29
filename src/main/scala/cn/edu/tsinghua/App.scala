package cn.edu.tsinghua
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.streaming.OutputMode



/**
 * Hello world!
 *
 */
object App{
  def main(args: Array[String]): Unit = {
    // spark streaming
//    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
//    val ssc = new StreamingContext(conf, Seconds(1))
//
//    // Create a DStream that will connect to hostname:port, like localhost:9999
////    val lines = ssc.socketTextStream("localhost", 9999)
//    val lines = ssc.textFileStream("/Users/liuzhicheng/Desktop/git/SparkTest/src/test/resource");
//
//    // Split each line into words
//    val words = lines.flatMap(_.split(" "))
//    // Count each word in each batch
//    val pairs = words.map(word => (word, 1))
//    val wordCounts = pairs.reduceByKey(_ + _)
//
//    // Print the first ten elements of each RDD generated in this DStream to the console
//    wordCounts.print()
//
//    ssc.start()             // Start the computation
//    ssc.awaitTermination()  // Wait for the computation to terminate

    // Structured Streaming
//    val spark = SparkSession
//      .builder()
//      .appName("Spark SQL basic example")
//      .master("local[2]")
//      .getOrCreate()
////    val df = spark.read.json("/Users/liuzhicheng/Desktop/git/SparkTest/src/test/resource/person.json")
////    df.show()
//
//    val userSchema = new StructType().add("name", "string").add("age", "integer")
//    val csvDF = spark
//      .readStream
//      .option("sep", ",")//name和age分隔符
//      .schema(userSchema)      // Specify schema of the csv files
//      .json("/Users/liuzhicheng/Desktop/git/SparkTest/src/test/resource")    // Equivalent to format("csv").load("file:///home/hdfs/data/structured-streaming")
//    //    // Count each word in each batch
//    //    val pairs = words.map(word => (word, 1))
//    //    val wordCounts = pairs.reduceByKey(_ + _)
//    //
//    //    // Print the first ten elements of each RDD generated in this DStream to the console
//    //    wordCounts.print()
////    // Returns True for DataFrames that have streaming sources
////    println("=================DataFrames中已经有流=================|"+csvDF.isStreaming +"|")
////    //打印csvDF模式
////    csvDF.printSchema
////    //执行结构化查询，将结果写入控制台console，输出模式为Append
//    val query = csvDF.writeStream
//      .outputMode(OutputMode.Append)
//      .format("console")
//      .trigger(Trigger.ProcessingTime("1 seconds"))
//      .start()
//    query.awaitTermination()

    // Spark Streaming
//    val conf = new SparkConf().setMaster("local[2]").setAppName("Spark Streaming")
//    val ssc = new StreamingContext(conf, Seconds(1))
//
//    val lines = ssc.textFileStream("/Users/liuzhicheng/Desktop/git/SparkTest/src/test/resource")
////    val lines = ssc.socketTextStream("localhost", 9999)
//    lines.print()
//    println("no data")
//    val words = lines.flatMap(_.split(" "))
//    // Count each word in each batch
//    val pairs = words.map(word => (word, 1))
//    val wordCounts = pairs.reduceByKey(_ + _)
//
//    // Print the first ten elements of each RDD generated in this DStream to the console
//    wordCounts.print()
//
//    ssc.start()             // Start the computation
//    ssc.awaitTermination()  // Wait for the computation to termination
//    ssc.stop()

//    val conf = new SparkConf().setAppName("spark").setMaster("local[*]")
//    val sparkContext = new SparkContext(conf)
    val spark = SparkSession.builder()
      .appName("Structured Streaming")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val df = Range(0, 10)
      .toList
      .map(value=>(value, value + 2))
      .toDF()
  }
}
