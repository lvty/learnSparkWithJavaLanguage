package com.spark.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.*;

public class GropTop3 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("LineCountLocal");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> inputRDD = sc.textFile("F:\\idea\\learnSparkWithJavaLanguage\\src\\score.txt",1);

        JavaPairRDD<String, Integer> groupedRDD = inputRDD.mapToPair(new PairFunction<String, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s.split(" ")[0], Integer.valueOf(s.split(" ")[1]));
            }
        });

        JavaPairRDD<String, Iterable<Integer>> groupByKeyRDD = groupedRDD.groupByKey();
        JavaPairRDD<String, Iterable<Integer>> groupedTop3 = groupByKeyRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, String, Iterable<Integer>>() {
            @Override
            public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> classAndScores) throws Exception {
                Integer[] top3 = new Integer[3];

                String className = classAndScores._1;
                Iterable<Integer> scores = classAndScores._2;

                Queue<Integer> queue = new PriorityQueue<Integer>(3);


                for (Integer i : scores) {
                    //如果当前队列元素个数小于3，直接插入
                    if(queue.size() < 3){
                        queue.add(i);
                    }else{
                        //判断队头元素与当前元素的大小，如果当前元素大于队头元素，则直接插入
                        if(queue.peek() < i){
                            queue.poll();
                            queue.offer(i);
                        }
                    }
                }
                int i =0;
                while(!queue.isEmpty() && i< 3){
                    top3[i++] = queue.poll();
                }

                return new Tuple2<String, Iterable<Integer>>(className, Arrays.asList(top3));
            }
        });

        groupedTop3.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> t) throws Exception {
                System.out.println("class: " + t._1);
                Iterator<Integer> it = t._2.iterator();
                while(it.hasNext()){
                    System.out.println("score: " + it.next());
                }
                System.out.println("=======================");
            }
        });

        sc.stop();
    }
}
