package com.spark.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

//与sparkSQL整合使用，实现top3热门商品实时统计
public class Top3HotProducts {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        //输入日志的格式  leo iphone mobile_phone  (某种类的某个商品)[用户名 商品 种类]

        //获取输入数据流
        JavaReceiverInputDStream<String> inDStream = jssc.socketTextStream("localhost", 9999);

        //将输入数据流做一个映射，将每个种类的每一个商品，映射为(category_product,1)的格式，从而在后面的window操作中，聚合操作，
        //统计出来，一个窗口中的每个种类的每个商品的点击次数
        JavaPairDStream<String, Integer> categoryProductCountsDStream = inDStream.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] split = s.split(" ");

                return new Tuple2<String, Integer>(split[2] + "_" + split[1], 1);
            }
        }).reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        }, Durations.seconds(60), Durations.seconds(10));

        //针对60秒内的每个种类的每个商品的点击次数，使用foreachRDD在内部使用SparkSQL执行top 3热门商品的统计
        categoryProductCountsDStream.foreachRDD(new Function<JavaPairRDD<String, Integer>, Void>() {
            @Override
            public Void call(JavaPairRDD<String, Integer> categoryProductCountsRDD) throws Exception {

                //转换为JavaRDD<Row>格式
                JavaRDD<Row> mapRDD = categoryProductCountsRDD.map(new Function<Tuple2<String, Integer>, Row>() {
                    @Override
                    public Row call(Tuple2<String, Integer> categoryProductCounts) throws Exception {
                        String[] split = categoryProductCounts._1.split("_");
                        String category = split[0];
                        String product = split[1];
                        Integer count = categoryProductCounts._2;
                        return RowFactory.create(category, product, count);
                    }
                });

                //执行DataFrame的转换
                List<StructField> fields = Arrays.asList(
                        DataTypes.createStructField("category", DataTypes.StringType, true),
                        DataTypes.createStructField("product", DataTypes.StringType, true),
                        DataTypes.createStructField("count", DataTypes.IntegerType, true)
                );

                StructType structType = DataTypes.createStructType(fields);

                //注意，注意，创建sqlcontext的方式
                HiveContext hiveContext = new HiveContext(categoryProductCountsRDD.context());
                DataFrame cpcDF = hiveContext.createDataFrame(mapRDD, structType);

                //将60秒内的每个种类的每个商品的点击次数的数据注册为一个临时表
                cpcDF.registerTempTable("cpc_table");

                //执行SQL语句，针对临时表，统计出来每个种类下，点击次数排名前3的热门商品
                DataFrame top3DF = hiveContext.sql(
                        "select category,product,count from ( " +
                                "select category,product,count," +
                                "row_number() over (partition by category order by count desc ) rank " +
                                "from cpc_table " +
                                ") temp where temp.rank <= 3");

                top3DF.show();


                return null;
            }
        });


        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
