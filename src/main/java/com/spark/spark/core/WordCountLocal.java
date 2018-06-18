package com.spark.spark.core;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;

//实现本地测试的Wordcount程序
public class WordCountLocal {

    public static void main(String[] args) {
        //编写spark应用程序

        /**
         * 1.创建sparkconf 对象，设置spark应用的配置信息;
         * setmaster 可以设置spark应用程序要连接的spark集群的master节点的url，但是如果设置为local则代表在本地执行
         *
         * 集群运行需要将setmaster()给删除，默认会自己去连接
         * 针对的不是本地文件了，修改为hadoop HDFS上的真正的存储大数据的文件
         */
        SparkConf conf = new SparkConf().setAppName("wordcountlocal").setMaster("local");
        //2.创建javaSparkContext对象
        /**sparkContext是所有功能的入口，无论使用java、scala、甚至是python编写的spark程序都必须有一个sparkContext
         *      它的主要作用是，包含初始化spark应用程序所需的核心组件，包括调度器（DAGScheduler,TaskScheduler），还会到spark master节点上进行注册
         * sparkcontext是spark应用中，可以说是比较重要的一个对象
         * 但是，在spark中，编写不同类型的spark应用程序，使用sparkcontext是不同的，
         *          如果使用scala，那么久使用原生的sparkContext对象
         *          使用java，那么久使用的是javasparkcontext对象
         *          使用sparkSQL程序，那么就是SQLcontext、hivecontext
         *          使用streamingcontext，那么。。
         *          以此类推。。
         */

        JavaSparkContext sc = new JavaSparkContext(conf);

        //3.针对输入源创建初始RDD
        JavaRDD<String> inputsRDD = sc.textFile("F://idea//learnSparkWithJavaLanguage//src//wc.txt");
        //4.对输入RDD操作
        /**
         * 通常操作会通过创建function，并配合RDD的map、flatmap等算子来执行
         * function，通常，简化书写的情况下，则创建指定的function的匿名内部类
         * 但是如果function比较复杂，则会单独创建一个类，作为实现某个function接口的类
         *
         *对于flatmap，先将每一行拆分成单个单词，FlatMapfunction有两个泛型参数，分别代表输入类型和输出类型
         */
        JavaRDD<String> words = inputsRDD.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(","));
            }
        });

        JavaPairRDD<String, Integer> wordsAnd1 = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s,1);
            }

        });


        JavaPairRDD<String, Integer> wordsAndVs = wordsAnd1.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static  final long serialVersionUID = 1L;

            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        JavaPairRDD<String, Integer> sortedValues = wordsAndVs.sortByKey(false);

        //先进行单词与次数的反转之后，对个数排序
        JavaPairRDD<Integer, String> res1 = wordsAndVs.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return new Tuple2<Integer, String>(stringIntegerTuple2._2, stringIntegerTuple2._1);
            }
        });

        JavaPairRDD<Integer, String> sortedRes = res1.sortByKey(false);
        sortedRes.mapToPair(new PairFunction<Tuple2<Integer,String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                return new Tuple2<String, Integer>(integerStringTuple2._2,integerStringTuple2._1);
            }
        }).foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1 +"times: " + t._2);
            }
        });

        /*sortedValues.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2._1 + "\tappears " +stringIntegerTuple2._2 + "\ttimes");
            }
        });*/


        sc.close();
    }
}
