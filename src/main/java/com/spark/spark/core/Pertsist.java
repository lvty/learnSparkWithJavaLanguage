package com.spark.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Pertsist {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("LineCountLocal");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("F://idea//learnSparkWithJavaLanguage//src//wc.txt").cache();
        long starttime = System.currentTimeMillis();
        long count = lines.count();
        System.out.println(count);
        long endtime = System.currentTimeMillis();
        System.out.println("耗时:" + (endtime - starttime) + "mils");


        starttime = System.currentTimeMillis();
        count = lines.count();
        System.out.println(count);
        endtime = System.currentTimeMillis();
        System.out.println("耗时:" + (endtime - starttime) + "mils");

        sc.close();
    }
}
