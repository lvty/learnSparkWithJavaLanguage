package com.spark.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

public class RowNumberFunction {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("");
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext hiveContext = new HiveContext(sc.sc());

        hiveContext.sql("drop table if exists sales");
        hiveContext.sql("create table if not exists sales(product String,category string,revenue bigint)");
        hiveContext.sql("load data local inpath '/root/sparkstudy/java/resource/sales.txt' " +
                "into table sales");

        //编写统计逻辑，使用row_number()开窗函数，给每个分组的数据，按照其排序顺序，打上一个组内行号，行号从1开始
        DataFrame top3DF = hiveContext.sql(
                "select product,category,revenue " +
                "from (" +
                    "select " +
                        " product," +
                        " category," +
                        " revenue," +
                        //语法说明：在select查询时可以使用row_number函数，先跟上over关键字，按照某个字段分组，组内排序
                        " row_number() over (partition by category order by revenue desc) rank" +
                        " from sales" +
                ") temp" +
                " where temp.rank <= 3");

        //将每组排名前三的数据保存至一张表中
        hiveContext.sql("drop table if exists top3_sales");
        top3DF.saveAsTable("top3_sales");
        sc.close();
    }
}
