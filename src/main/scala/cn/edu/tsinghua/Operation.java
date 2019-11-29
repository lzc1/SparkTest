package cn.edu.tsinghua;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import scala.Tuple2;

import java.util.*;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode_outer;

public class Operation {
  public static void main(String[] args) {
    SparkSession sparkSession = SparkSession.builder()
      .appName("Operation test")
      .master("local[*]")
      .getOrCreate();
    String filePath = "/Users/liuzhicheng/Desktop/git/SparkTest/src/main/resource/test.txt";
    Dataset<Row> df = sparkSession.read().csv(filePath);
////    Dataset<String> dfCount = df.map((MapFunction<String, String>) row -> row + ":1",
////        Encoders.STRING()
////    );
////    dfCount.show();
////
////    JavaRDD<String> lines = df.toJavaRDD();
////
////    JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
////      @Override
////      public Iterator<String> call(String s) throws Exception {
////        String[] words = s.split(" ");
////        return Arrays.asList(words).iterator();
////      }
////    });
////
////    words.foreach(s->System.out.println(s));
////    JavaPairRDD<String, Integer> mapResult = words.mapToPair(new PairFunction<String, String, Integer>(){
////      @Override
////      public Tuple2<String, Integer> call(String s) throws Exception {
////        return new Tuple2<String, Integer>(s, 1);
////      }
////    });
////    mapResult.foreach(s->System.out.println(s));
////    JavaRDD<Integer> length = words.map(word->word.length());
////    length.foreach(s->System.out.println(s));
////
////    JavaRDD<Integer> lineLengths = lines.map(new Function<String, Integer>() {
////      public Integer call(String s) { return s.length(); }
////    });
////    lineLengths.foreach(s->System.out.println(s));
//
//    // 细粒度计算
//    String filePath = "/Users/liuzhicheng/Desktop/git/SparkTest/src/test/resource/data.csv";
//    Dataset<Row> df = sparkSession.read().option("header", "true").csv(filePath);
//    df.show();
//    JavaPairRDD<String, Iterable<Row>> result = df.javaRDD().groupBy(row -> {
//      return row.get(1).toString();  // require String type
//    });
//
//    JavaRDD<Tuple2<String, Iterator<Row>>> windowResultRdd = result.map(new Function<Tuple2<String, Iterable<Row>>, Tuple2<String, Iterator<Row>>>() {
//      @Override
//      public Tuple2<String, Iterator<Row>> call(Tuple2<String, Iterable<Row>> stringIterableTuple2) throws Exception {
//        Iterable<Row> rows = stringIterableTuple2._2;
//        int count = 0;
//        Row lastRow = new RowFactory().create(0, 0, 0);
//        List<Row> resultRows = new ArrayList<>();
//        for (Row row:rows) {
//          if (count == 0){
//            lastRow = row;
//            count = 1;
//            continue;
//          }
//          int minus = Integer.parseInt(row.get(2).toString()) - Integer.parseInt(lastRow.get(2).toString());
//          resultRows.add(new RowFactory().create(minus));
//          lastRow = row;
//        }
//        return new Tuple2<>(stringIterableTuple2._1, resultRows.iterator());
//      }
//    });
////    JavaRDD<Tuple2<String, Map>> windowResultRdd = result.map(new Function<Tuple2<String, Iterable<Row>>, Tuple2<String, Map>>() {
////      @Override
////      public Tuple2<String, Map> call(Tuple2<String, Iterable<Row>> stringIterableTuple2) throws Exception {
////        Iterable<Row> rows = stringIterableTuple2._2;
////        Map<Object, Integer> count = new HashMap<>();
////        for (Row row : rows) {
////          if (count.keySet().contains(row.get(1))){
////            count.put(row.get(1), count.get(row.get(1)) + 1);
////          }else {
////            count.put(row.get(1), 1);
////          }
////        }
////        return new Tuple2<>(stringIterableTuple2._1, count);
////      }
////    });
//    windowResultRdd.foreach(rdd-> System.out.println(rdd._2.next().get(0)));

//    String[] columns = df.columns();
//    for (int i = 0; i < columns.length; i++) {
//      System.out.println(columns[i]);
//    }
//
//    Map<String, String> expr = new HashMap<>();
//    expr.put("value", "max");  // max
//    expr.put("value", "min");  // min
//    expr.put("value", "count");  // count
//    expr.put("value", "std");  // std
//    expr.put("value", "avg");  // average
//    df.agg(expr).show();
//    System.out.println(df.filter(col("value").equalTo("0")).count());  // zero

//    Quality.hi();
  }
}
