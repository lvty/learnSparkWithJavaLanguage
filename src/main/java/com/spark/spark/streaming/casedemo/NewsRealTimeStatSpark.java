package com.spark.spark.streaming.casedemo;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

//新闻网站实时指标统计
public class NewsRealTimeStatSpark {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("KafkaDirectWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

        //创建一份kafka参数的map
        HashMap<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list","node1:9092,node2:9092,node3:9092");

        //创建一个set，放入要读取的topic，这里就是所说的，它自己内部已经做的很好了，可以并行读取多个topic
        HashSet<String> topics = new HashSet<String>();
        topics.add("news-access");
        JavaPairInputDStream<String, String> inDStream = KafkaUtils.createDirectStream(jssc,
                String.class, String.class,
                StringDecoder.class, StringDecoder.class,
                kafkaParams, topics);

        //从所有数据中过滤出来，访问日志
        JavaPairDStream<String, String> accessDStream = inDStream.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> t) throws Exception {
                //2018-05-31 1527773772218 720 861 entertainment view
                return "view".equals(t._2.split(" ")[5]);
            }
        });
        System.out.println("统计第一个指标：每10秒内的各个页面的pv");
        calculateAccessPV(accessDStream);

        System.out.println("统计第二个指标：每10秒内的各个页面的uv");
        calculateAccessUV(accessDStream);

        System.out.println("统计第三个指标：每10秒内的注册用户数");
        calculateRegisterCount(inDStream);

        System.out.println("统计第四个指标：每10秒内用户跳出率，当前十秒内只访问了一次");
        calculateUserJumpCount(accessDStream);

        System.out.println("统计第五个指标：版块pv实时统计");
        calculateSectionPV(accessDStream);

        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }

    private static void calculateSectionPV(JavaPairDStream<String, String> accessDStream) {
        JavaPairDStream<String, Long> pairidDStream = accessDStream.mapToPair(new PairFunction<Tuple2<String, String>, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Tuple2<String, String> t) throws Exception {
                String[] split = t._2.split(" ");
                String pageId = String.valueOf(split[4]);
                return new Tuple2<String, Long>(pageId, 1L);
            }
        });
        pairidDStream.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long aLong, Long aLong2) throws Exception {
                return aLong +aLong2;
            }
        }).print();

    }

    //计算用户跳出数量
    private static void calculateUserJumpCount(JavaPairDStream<String, String> accessDStream) {
        accessDStream.mapToPair(new PairFunction<Tuple2<String,String>, Long, Long>() {
            @Override
            public Tuple2<Long, Long> call(Tuple2<String, String> t) throws Exception {
                String[] split = t._2.split(" ");
                Long userid = Long.valueOf("null".equals(split[2]) ? "-1":split[2]);
                return new Tuple2<>(userid,1L);
            }
        }).reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long aLong, Long aLong2) throws Exception {
                return aLong + aLong2;
            }
        }).filter(new Function<Tuple2<Long, Long>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Long, Long> t) throws Exception {
                return t._2 == 1;
            }
        }).count().print();
    }

    //每10秒内的注册用户数
    private static void calculateRegisterCount(JavaPairInputDStream<String, String> inDStream) {
        JavaPairDStream<String, String> registerDStream = inDStream.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> t) throws Exception {
                return "register".equals(t._2.split(" ")[5]);
            }
        });

        registerDStream.count().print();
        //每次统计完一个最近10秒的数据之后，不是打印出来而是去存储到(mysql redis hbase )，
        // 选用任何一种需要考虑是公司提供的工作环境以及实时报表的用户数量以及并发数量，包括你的数据量
        //如果是一般的展示效果就选用mysql就可以
        //如果是需要超高并发的展示，比如QPS 1W来看实时报表，那么建议使用Redis 或者memcached
        //如果是数据量特别大，建议使用HBASE

        //每次从存储，查询注册数量，最近一次插入的记录，比如上一次是10秒以前，然后将当前记录上一次的记录累加然后往存储中插入一条新纪录，就是最新的一条数据集
        //java EE展示时候就可以查看最近半小时的注册用户数量变化的曲线图
        //查看一周内，每天的注册用户数量的变化曲线图(每天就取最后一条数据就是每天的最终数据)
    }

    //计算页面UV
    private static void calculateAccessUV(JavaPairDStream<String, String> accessDStream) {
        JavaDStream<String> pageid_Userid = accessDStream.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> t) throws Exception {
                String[] split = t._2.split(" ");
                //针对pageid + userid 进行去重
                Long pageid = Long.valueOf(split[3]);
                Long userid = Long.valueOf("null".equals(split[2]) ? "-1":split[2]);
                return pageid + "_" + userid;
            }
        });

        pageid_Userid.transform(new Function<JavaRDD<String>, JavaRDD<String>>() {
            @Override
            public JavaRDD<String> call(JavaRDD<String> rdd) throws Exception {
                //去重操作
                return rdd.distinct();
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {

                return new Tuple2<String, Integer>(s.split("_")[0], 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        }).print();

    }

    //计算页面pv
    private static void calculateAccessPV(JavaPairDStream<String, String> accessDStream) {
        JavaPairDStream<Long, Long> pairidDStream = accessDStream.mapToPair(new PairFunction<Tuple2<String, String>, Long, Long>() {
            @Override
            public Tuple2<Long, Long> call(Tuple2<String, String> t) throws Exception {
                String[] split = t._2.split(" ");
                Long pageId = Long.valueOf(split[3]);
                return new Tuple2<Long, Long>(pageId, 1L);
            }
        });
        pairidDStream.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long aLong, Long aLong2) throws Exception {
                return aLong +aLong2;
            }
        }).print();

        //在真实项目中，在每次计算出之后应该持久化到mysql或者Redis中，对每个页面的pv进行累加；在javaee系统中就可以从mysql或者Redis中读取
        //pv实时变化的数据，以及曲线图
    }
}
