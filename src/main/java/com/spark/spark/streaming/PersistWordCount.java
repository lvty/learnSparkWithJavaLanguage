package com.spark.spark.streaming;

import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import java.net.ConnectException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

//基于持久化的Wordcount程序
public class PersistWordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("HDFSWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

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

        //每次得到当前单词的统计次数之后，将其写入mysql持久化存储，以便于后续的J2EE应用程序显示
        res.foreachRDD(new Function<JavaPairRDD<String, Integer>, Void>() {
            @Override
            public Void call(JavaPairRDD<String, Integer> wc) throws Exception {
                wc.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Integer>> t) throws Exception {
                        //针对每一个partition获取一个连接，持久化到mysql中
                        Connection connection = ConnectionPool.getConnection();
                        Tuple2<String, Integer> wc = null;
                        String sql = "insert into wordcount(word,count) values(?,?)";
                        while(t.hasNext()){
                            wc = t.next();
                            PreparedStatement psmt = connection.prepareStatement(sql);
                            psmt.setString(1,wc._1);
                            psmt.setInt(2,wc._2);
                            psmt.executeUpdate();
                        }
                        //用完就还回去
                        ConnectionPool.returnConnection(connection);
                    }
                });
                return null;
            }
        });

        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
