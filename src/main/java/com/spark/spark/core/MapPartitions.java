package com.spark.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.*;

public class MapPartitions {
    public static void main(String[] args) {

        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setMaster("local[2]").setAppName(""));
        List<Integer> list = Arrays.asList(1, 2, 3, 4);
        JavaRDD<Integer> studentsRDD = sc.parallelize(list, 2);
        final HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();
        map.put(1,98);
        map.put(2,99);
        map.put(3,101);
        map.put(4,100);

        //mappartitions与map的区别：
        //map 一次只处理一个partition中的一条数据，而mappartitions算子，一次处理一个partition中所有的算子

        //如果partition数据量不是特别大，那么建议采用mappartitions算子替代map算子；否则，可能会引起内存溢出
        JavaRDD<Integer> res = studentsRDD.mapPartitions(new FlatMapFunction<Iterator<Integer>, Integer>() {
            @Override
            public Iterable<Integer> call(Iterator<Integer> it) throws Exception {
                //算子一次处理一个partition中所有数据
                ArrayList<Integer> list = new ArrayList<Integer>();
                while (it.hasNext()) {
                    Integer res = map.get(it.next());
                    list.add(res);
                }
                return list;
            }
        });
        res.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });


        sc.stop();
    }
}
