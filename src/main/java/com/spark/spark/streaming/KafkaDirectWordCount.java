package com.spark.spark.streaming;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

public class KafkaDirectWordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("KafkaDirectWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        //创建一份kafka参数的map
        HashMap<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list","node1:9092,node2:9092,node3:9092");

        //创建一个set，放入要读取的topic，这里就是所说的，它自己内部已经做的很好了，可以并行读取多个topic
        HashSet<String> topics = new HashSet<String>();
        topics.add("WC");
        JavaPairInputDStream<String, String> inDStream = KafkaUtils.createDirectStream(jssc,
                String.class, String.class,
                StringDecoder.class, StringDecoder.class,
                kafkaParams, topics);

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
