package com.spark.spark.streaming;

import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

//基于updatestateByKey算子实现缓存机制的实时Wordcount程序
public class UpdateStateByKeyWordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("HDFSWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        //第一点，如果要使用updateStateByKey算子，就必须设置一个checkpoint目录，开启checkpoint机制
        //这样的话才能把每一个key对应的state除了在内存中有以外，那么是不是也要checkpoint一份，因为要长期
        //保存一份key的state的话，那么spark streaming是要求必须checkpoint的，以便在内存数据丢失的时候
        //可以从checkpoint中恢复数据。

        //开启
        jssc.checkpoint("hdfs://ll/wordcount_checkpoint");

        JavaReceiverInputDStream<String> inDStream = jssc.socketTextStream("node1", 9999);

        JavaPairDStream<String, Integer> res = inDStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        }).updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {

            //Optional相当于Scala中的样例类，就是option，可以这样理解，它代表了一个值的存在状态，可能存在，也可能不存在
            //实际上，对于每个单词，每次batch计算的时候，都会调用这个函数，第一个参数，values相当于在这个batch中，这个key中的新的值，
            // 可能有多个吧，比如说一个hello，在这个batch中有2个，(hello,1)(hello,1)，那么传入的是(1,1)；
            // 第二个参数，就是指的是这个key之前的状态，state，其中泛型的类型就是自己指定的类型
            public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
                //首先定义一个全局的单词计数
                Integer newValue = 0;

                //其次，判断单词的state是否存在，如果不存在，说明是一个key第一次出现，如果存在，说明这个key之前已经统计过全局的次数了
                if (state.isPresent()) {
                    newValue = state.get();
                }

                //接着，将本次新出现的值，都累加到newvalue上去，就是一个key目前的全局的统计次数
                for (Integer i : values) {
                    newValue += i;
                }
                return Optional.of(newValue);
            }
        });

        //如果要求统计全局的单词计数呢？就是说，统计出来，从程序启动开始，到现在为止，一个单词出现的次数，那么就之前的方式就不好实现了，
        // 就必须基于Redis这种缓存机制或者是mysql这样的DB来实现累加功能，但是，updateStateByKey就可以实现直接通过Spark维护一份
        // 每个单词的全局的统计次数。

        res.print();

        jssc.start();
        jssc.awaitTermination();
        jssc.close();

    }

}
