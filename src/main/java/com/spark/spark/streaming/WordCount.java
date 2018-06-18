package com.spark.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

//实时Wordcount程序
public class WordCount {
    public static void main(String[] args) {

        //master中一定要设置线程个数
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("WordCount");
        //创建javastreamingcontext对象,此外，还必须接收一个batch interval参数，也就是说，是每收集多长时间的数据，划分为一个batch，进行处理
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(conf, Durations.seconds(1));

        //创建输入dstream，代表了一个从数据源(比如kafka，socket)来的持续不断的实时数据流
        //调用jssc的sockettextstream方法，可以创建一个数据源为socket网络端口的数据流，javareceiverinputstream代表了输入的dstream
        JavaReceiverInputDStream<String> lines = javaStreamingContext.socketTextStream("localhost", 9999);
        //JavaReceiverInputDStream每隔一秒，会有一个RDD，其中封装了这一秒发送过来的数据。RDD的元素类型为String，即一行一行的文本
        //所以这里JavaReceiverInputDStream的泛型为String，其中就代表了它底层的RDD的泛型类型

        //开始对接收到的数据，执行计算，使用Sparkcore提供的算子，执行应用在dstream中即可，在底层，实际上是会对dstream中的
        //一个个RDD执行我们应用在dstream上的算子，产生新的RDD，会作为dstream中的RDD
        //每秒的数据，一行一行的文本，就会被拆分为多个单词，words dstream中的rdd的元素类型即为一个一个的单词
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });

        //接着开始进行map 和 reduceBykey
        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairDStream<String, Integer> wcres = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        //总结：要注意的是，SparkStreaming的计算模型决定了，我们必须自己来进行中间缓存的控制，比如写入Redis等缓存；
        // 它的计算模型和storm是完全不同的，storm是自己编写的一个一个的程序运行在节点上，相当于一个一个的对象，
        // 可以自己在对象中控制缓存；但是spark本身是函数式编程的计算模型，所以，比如在words 或者pairs dstream中，
        // 没法在实例变量找那个进行缓存，此时就只能讲最后计算出来的wordcounts中的一个个的RDD写入外部的缓存，或者持久化到DB中。

        //最后，每次计算完，都打印一下这一秒中的单词计数情况，并休眠5秒钟，以便我们测试和观察
        wcres.print();
        try {
            Thread.sleep(5);
        } catch (InterruptedException e) {
            e.printStackTrace();

        }

        //对JavaStreamingContext进行一下后续处理,必须调用Start方法，才会启动执行
        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
        javaStreamingContext.close();
    }
}
