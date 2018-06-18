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

public class Coalesce {
    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("").setMaster("local[2]"));

        List<Integer> list = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> studentsRDD = sc.parallelize(list, 5);

        //Coalesce 是将一定量的数据压缩到更少的partition中去，建议使用的场景时配合filter算子使用
        //使用filter算子过滤掉很多数据之后，比如30%的数据，出现了很多partition中数据不均匀的情况
        //此时建议使用coalesce算子，压缩rdd的partition数量

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
        studentsRDD.coalesce(3).mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<String>>() {
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
