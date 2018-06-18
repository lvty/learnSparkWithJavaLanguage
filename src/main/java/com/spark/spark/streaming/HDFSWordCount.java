package com.spark.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

//基于HDFS文件的实时Wordcount程序
public class HDFSWordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("HDFSWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));


        //使用JavaStreamingContext的filestream创建输入数据流
        JavaDStream<String> linesjavaDStream = jssc.textFileStream("hdfs://ll/wc_dir");
        //执行wordcount 
        JavaPairDStream<String, Integer> res = linesjavaDStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {

                return integer + integer2;
            }
        });

        res.print();


        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
