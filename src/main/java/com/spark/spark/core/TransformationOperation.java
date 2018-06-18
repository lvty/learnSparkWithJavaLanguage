package com.spark.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class TransformationOperation {
    public static void main(String[] args) {
        //map();
        //filter();
        //flatMap();
        //groupByKey();
        //reduceBykey();
        //ortByKey();
        //join();
        cogroup();
    }

    public static void map(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("LineCountLocal");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> pRDD = sc.parallelize(list);
        JavaRDD<Integer> res = pRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                return integer * 2;
            }
        });
        res.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });

        sc.close();
    }

    public static void filter(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("LineCountLocal");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> pRDD = sc.parallelize(list);
        JavaRDD<Integer> res = pRDD.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer integer) throws Exception {
                return (integer % 2 == 0);
            }
        });
        res.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });

        sc.close();
    }

    public static void flatMap(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("LineCountLocal");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> list = Arrays.asList("hello him","hello you","hello me");
        JavaRDD<String> pRDD = sc.parallelize(list);

        JavaRDD<String> res = pRDD.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });
        res.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        sc.close();
    }

    /**
     * 按照班级对成绩进行分组
     */
    public static void groupByKey(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("LineCountLocal");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String,Integer>> list = Arrays.asList(new Tuple2<String, Integer>("class1",80),
                new Tuple2<String, Integer>("class2",60),
                new Tuple2<String, Integer>("class1",70),
                new Tuple2<String, Integer>("class2",90));

        JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(list, 2);
        JavaPairRDD<String, Iterable<Integer>> res = pairRDD.groupByKey();

        res.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> t) throws Exception {
                System.out.println("class: " + t._1);
                Iterator<Integer> iterator = t._2.iterator();
                while(iterator.hasNext()){
                    System.out.println(iterator.next());
                }
                System.out.println("+++++++++");
            }
        });


        sc.close();
    }

    //统计班级总分
    public static void reduceBykey(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("LineCountLocal");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String,Integer>> list = Arrays.asList(new Tuple2<String, Integer>("class1",80),
                new Tuple2<String, Integer>("class2",60),
                new Tuple2<String, Integer>("class1",70),
                new Tuple2<String, Integer>("class2",90));

        JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(list, 2);
        JavaPairRDD<String, Integer> res = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        res.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1 + "总成绩是: " + t._2);
            }
        });


        sc.close();
    }

    public static void sortByKey(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("LineCountLocal");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<Integer,String>> list = Arrays.asList(new Tuple2<Integer,String>(80,"s1"),
                new Tuple2<Integer,String>(60,"s2"),
                new Tuple2<Integer,String>(70,"s3"),
                new Tuple2<Integer,String>(90,"s4"));

        JavaPairRDD<Integer,String> rdd = sc.parallelizePairs(list);
        JavaPairRDD<Integer, String> res = rdd.sortByKey(false);
        res.foreach(new VoidFunction<Tuple2<Integer, String>>() {
            @Override
            public void call(Tuple2<Integer, String> t) throws Exception {
                System.out.println(t._1 + t._2);
            }
        });


        sc.close();
    }

    public static void join(){

        SparkConf conf = new SparkConf().setMaster("local").setAppName("LineCountLocal");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<Integer,String>> students = Arrays.asList(new Tuple2<Integer,String>(1,"s1"),
                new Tuple2<Integer,String>(1,"s2"),
                new Tuple2<Integer,String>(2,"s3"));

        List<Tuple2<Integer,String>> scores = Arrays.asList(new Tuple2<Integer,String>(1,"100"),
                new Tuple2<Integer,String>(2,"90"),
                new Tuple2<Integer,String>(2,"60"),
                new Tuple2<Integer,String>(3,"40"));

        JavaPairRDD<Integer, String> st = sc.parallelizePairs(students);
        JavaPairRDD<Integer, String> ss = sc.parallelizePairs(scores);

        JavaPairRDD<Integer, Tuple2<String, String>> res = st.join(ss);
        res.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, String>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<String, String>> t) throws Exception {
                System.out.println("id:" + t._1 + "\t name: " + t._2._1 + "\t score: " + t._2._2);
            }
        });

        sc.close();

    }

    public static void cogroup(){

        SparkConf conf = new SparkConf().setMaster("local").setAppName("LineCountLocal");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<Integer,String>> students = Arrays.asList(new Tuple2<Integer,String>(1,"s1"),
                new Tuple2<Integer,String>(1,"s2"),
                new Tuple2<Integer,String>(2,"s3"));

        List<Tuple2<Integer,String>> scores = Arrays.asList(new Tuple2<Integer,String>(1,"100"),
                new Tuple2<Integer,String>(2,"90"),
                new Tuple2<Integer,String>(2,"60"),
                new Tuple2<Integer,String>(3,"40"));

        JavaPairRDD<Integer, String> st = sc.parallelizePairs(students);
        JavaPairRDD<Integer, String> ss = sc.parallelizePairs(scores);

        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<String>>> res = st.cogroup(ss);
        res.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable<String>, Iterable<String>>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<String>>> t) throws Exception {
                Integer id = t._1;
                Iterable<String> st = t._2._1;
                Iterable<String> sc = t._2._2;
                StringBuffer sb = new StringBuffer();
                sb.append("id: " + id);
                for(String s:st){
                    sb.append("\t stValues:" + s);
                }
                for(String s:sc){
                    sb.append("\t scValues:" + s);
                }
                System.out.println(sb.toString());
            }
        });

        sc.close();

    }

}
