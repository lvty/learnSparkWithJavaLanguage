package com.spark.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;
import scala.Tuple2;
import java.util.Arrays;


public class FlumePushWordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("KafkaDirectWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        JavaReceiverInputDStream<SparkFlumeEvent> lines = FlumeUtils.createStream(jssc, "192.168.1.14", 8888);

        JavaPairDStream<String, Integer> res = lines.flatMap(new FlatMapFunction<SparkFlumeEvent, String>() {
            @Override
            public Iterable<String> call(SparkFlumeEvent event) throws Exception {
                String s = new String(event.event().getBody().array());
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
