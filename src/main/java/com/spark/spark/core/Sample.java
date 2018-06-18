package com.spark.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class Sample {
    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("").setMaster("local[2]"));

        List<Integer> list = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> studentsRDD = sc.parallelize(list, 2);

        //Sample使用指定比例抽取，是transformation操作；而takeSample是action操作，只能指定抽取几个数据
        List<Integer> res = studentsRDD.sample(true, 0.1).collect();
        for(Integer i:res){
            System.out.println(i);
        }


        sc.close();
    }
}
