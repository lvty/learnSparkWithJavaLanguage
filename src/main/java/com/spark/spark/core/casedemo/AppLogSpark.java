package com.spark.spark.core.casedemo;

import com.spark.spark.core.casedemo.AccessInfo;
import com.spark.spark.core.casedemo.AccessLogSortKey;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

//移动端访问日志分析案例
public class AppLogSpark {
    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setMaster("local[2]").setAppName(""));
        JavaRDD<String> inRDD = sc.textFile("F:\\idea\\learnSparkWithJavaLanguage\\src\\data\\access.log");

        //将输入RDD映射为key_value格式，为后面的reduceByKey聚合做准备
        JavaPairRDD<String, AccessInfo> accessLogPairRDD = mapAccessLogRDD2Pair(inRDD);
        JavaPairRDD<String, AccessInfo> aggrAccessLogPairRDD = aggregateByDeviceID(accessLogPairRDD);
        JavaPairRDD<AccessLogSortKey, String> accessLogSortKeyJavaPairRDD = mapRDD2SortedKey(aggrAccessLogPairRDD);
        //执行二次排序，按照AccessLogSortKey倒序排序

        List<Tuple2<AccessLogSortKey, String>> list =
                accessLogSortKeyJavaPairRDD.sortByKey(false).take(10);

        for(Tuple2<AccessLogSortKey, String> t:list){
            System.out.println(t._1.toString() + "\tdeviceID: " + t._2);
        }

        sc.close();
    }

    //将RDD的key 映射为二次排序的key
    public static JavaPairRDD<AccessLogSortKey,String> mapRDD2SortedKey(JavaPairRDD<String, AccessInfo> aggrAccessLogPairRDD){
        return aggrAccessLogPairRDD.mapToPair(new PairFunction<Tuple2<String, AccessInfo>, AccessLogSortKey, String>() {
            @Override
            public Tuple2<AccessLogSortKey, String> call(Tuple2<String, AccessInfo> t) throws Exception {
                return new Tuple2<>(
                        new AccessLogSortKey(
                                t._2.getTimestamp(),
                                t._2.getUpFlow(),
                                t._2.getDownFlow()
                        ),
                        t._1);
            }
        });
    }



    //根据deviceID进行聚合操作，计算出每个deviceID总的上行流量/总下行流量和最早访问时间
    public static JavaPairRDD<String,AccessInfo> aggregateByDeviceID(JavaPairRDD<String, AccessInfo> inRDD){
        return inRDD.reduceByKey(new Function2<AccessInfo, AccessInfo, AccessInfo>() {
            @Override
            public AccessInfo call(AccessInfo ac1, AccessInfo ac2) throws Exception {
                return new AccessInfo(
                     Long.min(ac1.getTimestamp(),ac2.getTimestamp()),
                        ac1.getUpFlow() + ac2.getUpFlow(),
                        ac1.getDownFlow() + ac2.getDownFlow()
                );
            }
        });
    }


    public static JavaPairRDD<String,AccessInfo> mapAccessLogRDD2Pair(JavaRDD<String> inRDD){
        return inRDD.mapToPair(new PairFunction<String, String, AccessInfo>() {
            @Override
            public Tuple2<String, AccessInfo> call(String s) throws Exception {
                String[] split = s.split("\t");
                //1454307391161	77e3c9e1811d4fb291d0d9bbd456bb4b	79976	11496
                Long timestamp = Long.valueOf(split[0]);
                String deviceID = split[1];
                Long upFlow = Long.valueOf(split[2]);
                Long downFlow = Long.valueOf(split[3]);

                AccessInfo accessInfo = new AccessInfo(timestamp,upFlow,downFlow);
                return new Tuple2<String, AccessInfo>(deviceID,accessInfo);
            }
        });
    }
}
