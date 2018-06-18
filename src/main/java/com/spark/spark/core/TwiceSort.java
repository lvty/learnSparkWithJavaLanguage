package com.spark.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.io.Serializable;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;
import scala.math.Ordered;

/**
 * 二次排序：
 *      1.实现自定义的key，要实现ordered接口和serializable接口，在key中实现自己对多个列的排序算法
 *      2.将包含多个文件的RDD，映射为自定义key，value为文本的JavaPairRDD
 *      3.使用SortBykey算子按照自定义的key进行排序
 *      4.再次映射，剔除自定义的key，只保留文本行
 */

public class TwiceSort {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("LineCountLocal");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("F:\\idea\\learnSparkWithJavaLanguage\\src\\sort.txt");
        JavaPairRDD<SecondarySortKey, String> secondarySortKeyStringJavaPairRDD = lines.mapToPair(new PairFunction<String, SecondarySortKey, String>() {
            @Override
            public Tuple2<SecondarySortKey, String> call(String s) throws Exception {
                String[] split = s.split(" ");
                SecondarySortKey key = new SecondarySortKey(
                        Integer.valueOf(split[0]),
                        Integer.valueOf(split[1])
                );
                return new Tuple2<SecondarySortKey, String>(key, s);
            }
        });

        JavaPairRDD<SecondarySortKey, String> res = secondarySortKeyStringJavaPairRDD.sortByKey();
        JavaRDD<String> r = res.map(new Function<Tuple2<SecondarySortKey, String>, String>() {
            @Override
            public String call(Tuple2<SecondarySortKey, String> t) throws Exception {
                return t._2;
            }
        });
        r.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });


        sc.close();
    }

    static class SecondarySortKey implements Ordered<SecondarySortKey>, Serializable{
        private int first;
        private int second;

        public SecondarySortKey(int first, int second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public boolean $less(SecondarySortKey that) {
            if(this.first < that.getFirst()){
                return true;
            }else if(this.first == that.getFirst() && this.second < that.getSecond()){
                return true;
            }
            return false;
        }

        @Override
        public boolean $greater(SecondarySortKey that) {
            if(this.first > that.getFirst()){
                return true;
            }else if(this.first == that.getFirst() && this.second > that.getSecond()){
                return true;
            }
            return false;
        }

        @Override
        public boolean $less$eq(SecondarySortKey that) {
            if(this.$less(that)){
                return true;
            }else if(this.first == that.first && this.second == that.getSecond()){
                return true;
            }
            return false;
        }

        @Override
        public boolean $greater$eq(SecondarySortKey that) {
            if(this.$greater(that)){
                return true;
            }else if(this.first == that.first && this.second == that.getSecond()){
                return true;
            }
            return false;
        }


        @Override
        public int compare(SecondarySortKey that) {
            int diff = this.first - that.getFirst();
            if(diff != 0){
                return diff;
            }else{
                return this.second - that.getSecond();
            }
        }

        @Override
        public int compareTo(SecondarySortKey that) {
            int diff = this.first - that.getFirst();
            if(diff != 0){
                return diff;
            }else{
                return this.second - that.getSecond();
            }
        }

        //在自定义的key中定义中，要为进行排序的多个列，提供getter和setter方法，以及hashcode和equals方法


        public int getFirst() {
            return first;
        }

        public void setFirst(int first) {
            this.first = first;
        }

        public int getSecond() {
            return second;
        }

        public void setSecond(int second) {
            this.second = second;
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return super.equals(obj);
        }
    }
}
