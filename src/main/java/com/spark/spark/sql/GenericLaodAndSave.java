package com.spark.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class GenericLaodAndSave {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        DataFrame df = sqlContext.read()
                .load("F:\\idea\\learnSparkWithJavaLanguage\\src\\data\\users.parquet");

        df.select(df.col("name"),df.col("favorite_color"),df.col("favorite_numbers"))
                .write().save("F:\\idea\\learnSparkWithJavaLanguage\\src\\data\\res.txt");

        sc.close();
    }
}
