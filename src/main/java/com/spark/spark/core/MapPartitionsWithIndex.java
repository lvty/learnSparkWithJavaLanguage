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

public class MapPartitionsWithIndex {
    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("").setMaster("local[2]"));
        List<Integer> list = Arrays.asList(1, 2, 3, 4);
        JavaRDD<Integer> studentsRDD = sc.parallelize(list, 2);

        //如何获取每一个partition的index？
        JavaRDD<String> studentWothClassRDD = studentsRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<String>>() {

            @Override
            public Iterator<String> call(Integer integer, Iterator<Integer> iterator) throws Exception {

                ArrayList<String> studentWithClassList = new ArrayList<String>();
                while(iterator.hasNext()){
                    studentWithClassList.add(iterator.next() + "_" + integer);
                }
                return studentWithClassList.iterator();
            }
        },true);

        studentWothClassRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        sc.close();
    }
}
