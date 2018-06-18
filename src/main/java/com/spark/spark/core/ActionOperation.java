package com.spark.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ActionOperation {
    public static void main(String[] args) {
        //reduce();
        //collect();
        //count();
        //take();
        //saveAsTextFile();
        countBykey();
    }

    public static void reduce(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("ParallelizeCollection");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> list = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> javaRDD = sc.parallelize(list, 2);
        Integer res = javaRDD.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        System.out.println(res);
        sc.close();
    }

    public static void collect(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("ParallelizeCollection");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> list = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> javaRDD = sc.parallelize(list, 2);
        JavaRDD<Integer> map = javaRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                return integer * 2;
            }
        });
        List<Integer> res = map.collect();
        for(Integer i : res){
            System.out.println(i);
        }
        sc.close();
    }

    public static void count(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("ParallelizeCollection");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> list = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> javaRDD = sc.parallelize(list, 2);
        JavaRDD<Integer> map = javaRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                return integer * 2;
            }
        });
        System.out.println(map.count());

        sc.close();
    }

    public static void take(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("ParallelizeCollection");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> list = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> javaRDD = sc.parallelize(list, 2);
        JavaRDD<Integer> map = javaRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                return integer * 2;
            }
        });
        List<Integer> res = map.take(3);
        for(Integer i : res){
            System.out.println(i);
        }
        sc.close();
    }

    public static void saveAsTextFile(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("ParallelizeCollection");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> list = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> javaRDD = sc.parallelize(list, 2);
        JavaRDD<Integer> map = javaRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                return integer * 2;
            }
        });
        map.saveAsTextFile("F://idea//learnSparkWithJavaLanguage//src//saveFiles.txt");

        sc.close();
    }

    public static void countBykey(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("LineCountLocal");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String,Integer>> list = Arrays.asList(new Tuple2<String, Integer>("class1",80),
                new Tuple2<String, Integer>("class2",60),
                new Tuple2<String, Integer>("class1",70),
                new Tuple2<String, Integer>("class2",90));

        JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(list, 2);
        Map<String, Object> res = pairRDD.countByKey();
        for(Map.Entry<String,Object> studentcount: res.entrySet()){
            System.out.println(studentcount.getKey() + "\t" + studentcount.getValue());

        }

        sc.close();
    }
}
