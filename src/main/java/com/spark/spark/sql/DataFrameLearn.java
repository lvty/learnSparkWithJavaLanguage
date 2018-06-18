package com.spark.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

//创建DataFrame
public class DataFrameLearn {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        DataFrame df = sqlContext.read().format("json")
                .load("F:\\idea\\learnSparkWithJavaLanguage\\src\\data\\students.json");
        //打印元数据
        df.printSchema();
        //打印数据
        df.show();
        //查询某一列
        df.select("name").show();
        //查询多个列，并对某一列处理
        df.select(df.col("name"),df.col("age").plus(1)).show();
        //过滤操作
        df.filter(df.col("age").gt(18)).show();
        //分组操作
        df.groupBy(df.col("age")).count().show();
        sc.close();
    }
}
