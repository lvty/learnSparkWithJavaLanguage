package com.spark.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Repartition {
    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("").setMaster("local[2]"));

        List<Integer> list = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> studentsRDD = sc.parallelize(list, 5);

        //Repartition 用于将任意RDD的partition增多或者减少，与coalesce不同之处在于，coalesce仅仅能将rdd的partition变少
        //但是repartition可以将rdd的partition变多’
        //建议使用的场景是：使用spark SQL从hive中查询数据时，spark SQL会根据hive对应的HDFS文件的block数量决定加载出来的
        //数据RDD有多少个partition，这里的partition数量是我们根本无法进行设置的

        //有些时候，可能它自动设置的partition数量过于少了，导致我们后面的算子运行特别慢，
        // 这种场景就可以使用repartition算子将partition变多

        studentsRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<Integer> iterator) throws Exception {
                ArrayList<String> list = new ArrayList<String>();
                while(iterator.hasNext()){
                    list.add("value: " + iterator.next() + " index: " + index);
                }
                return list.iterator();
            }
        },true).foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
        System.out.println("============华丽的分割线====================");
        studentsRDD.repartition(3).mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<Integer> iterator) throws Exception {
                ArrayList<String> list = new ArrayList<String>();
                while(iterator.hasNext()){
                    list.add("value: " + iterator.next() + " index: " + index);
                }
                return list.iterator();
            }
        },true).foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
        sc.close();
    }
}
