package com.spark.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;

//使用kafka实现实时Wordcount
public class KafkaReceiverWordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("KafkaReceiverWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        //指定多少个线程去拉取partition上的数据
        HashMap<String, Integer> topicThreadMap = new HashMap<String, Integer>();
        topicThreadMap.put("WC",1);

        //使用kafkaUtils.createStream()方法，创建针对kafka的输入数据流
        JavaPairReceiverInputDStream<String, String> inDStream = KafkaUtils.createStream(jssc,
                "192.168.1.14:2181,192.168.1.13:2181,192.168.1.12:2181",
                "DefaultConsumerGroup",
                topicThreadMap);

        JavaPairDStream<String, Integer> res = inDStream.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
            @Override
            public Iterable<String> call(Tuple2<String, String> t) throws Exception {
                return Arrays.asList(t._2.split(" "));
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
