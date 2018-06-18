package com.spark.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.*;

//每日top3热点搜索词统计案例
public class DailyTop3KeyWordsCaseDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("DailyTop3KeyWordsCaseDemo");
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext hiveContext = new HiveContext(sc.sc());

        //1.获取输入RDD
        JavaRDD<String> inputRDD = sc.textFile("hdfs://ll/study_data/keyword.txt", 5);
        //伪造出一份数据，作为查询条件，在实际企业中，可能会通过J2EE平台插入到某一个MySQL表中的，
        // 然后，实际上，通常是会使用spring框架和orm框架，去提取mysql表中的查询条件
        HashMap<String, List<String>> queryParamMap = new HashMap<String, List<String>>();
        queryParamMap.put("city", Arrays.asList("beijing"));
        queryParamMap.put("platform", Arrays.asList("android"));
        queryParamMap.put("version", Arrays.asList("1.0","1.2","1.5","2.0"));
        //根据分析思路，将该过滤条件作为广播变量，这样每一个worker就只有iFeng数据即可
        final Broadcast<HashMap<String, List<String>>> queryParamMapBCVariable = sc.broadcast(queryParamMap);
        //使用查询参数广播变量进行筛选
        JavaRDD<String> filterConditionRDD = inputRDD.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                //切分原始日志
                String[] split = s.split("\t");
                String city = split[3];
                String platform = split[4];
                String version = split[5];
                HashMap<String, List<String>> temp = queryParamMapBCVariable.value();

                //判断城市
                List<String> cities = temp.get("city");
                if (cities.size() > 0 && !cities.contains(city)) {
                    return false;
                }

                //判断平台
                List<String> platforms = temp.get("platform");
                if (platforms.size() > 0 && !platforms.contains(platform)) {
                    return false;
                }

                List<String> versions = temp.get("version");
                if (versions.size() > 0 && !versions.contains(version)) {
                    return false;
                }

                return true;
            }
        });

        //3.过滤出来的原始日志，映射为(日期_搜索词，用户)的格式
        JavaPairRDD<String, String> date_userAndKeyWordRDD = filterConditionRDD.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                String[] split = s.split("\t");

                String date = split[0];
                String user = split[1];
                String keyword = split[2];

                return new Tuple2<String, String>(date + "_" + keyword, user);
            }
        });

        //4.进行分组，获取每天每个搜索词，有哪些用户搜索了(没有去重)
        JavaPairRDD<String, Iterable<String>> date_userAndKeyWordGroupedRDD = date_userAndKeyWordRDD.groupByKey();

        //5.对每天每个搜索词，去重操作，获取其UV
        JavaPairRDD<String, Long> dateAndKeyWordUVRDD = date_userAndKeyWordGroupedRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Tuple2<String, Iterable<String>> t) throws Exception {
                String dateAndKeyWord = t._1;
                Iterable<String> users = t._2;
                Set<String> set = new HashSet<String>();
                for (String s : users) {
                    set.add(s);
                }
                return new Tuple2<String, Long>(dateAndKeyWord, Long.valueOf(set.size()));
            }
        });

        //6.将UV数据转换为DataFrame
        JavaRDD<Row> dateAndKeyWordUVROWRDD = dateAndKeyWordUVRDD.map(new Function<Tuple2<String, Long>, Row>() {
            @Override
            public Row call(Tuple2<String, Long> t) throws Exception {
                String[] split = t._1.split("_");
                String date = split[0];
                String keyword = split[1];
                Long uv = t._2;
                return RowFactory.create(date, keyword, uv);
            }
        });

        List<StructField> structFields = Arrays.asList(
                DataTypes.createStructField("date", DataTypes.StringType, true),
                DataTypes.createStructField("keyword", DataTypes.StringType, true),
                DataTypes.createStructField("uv", DataTypes.LongType, true)
        );
        StructType structType = DataTypes.createStructType(structFields);
        DataFrame dateAndKeyWordUVDF = hiveContext.createDataFrame(dateAndKeyWordUVROWRDD, structType);

        //7.使用SparkSql的开窗函数，统计每天搜索UV排名前3的热点搜索词
        dateAndKeyWordUVDF.registerTempTable("dateAndKeyWordUVDF");

        String sql = "select date,keyword,uv " +
                "from (" +
                "select " +
                " date," +
                " keyword," +
                " uv," +
                //语法说明：在select查询时可以使用row_number函数，先跟上over关键字，按照某个字段分组，组内排序
                " row_number() OVER(partition by date order by uv desc) rank" +
                " from dateAndKeyWordUVDF" +
                ") temp" +
                " where temp.rank <= 3";

        final DataFrame dailyTop3KeyWordDF = hiveContext.sql(sql);

        //将DataFrame转换为rdd，然后映射，计算出每天的top3搜索词的搜索UV总数
        JavaPairRDD<String, String> top3DateKeyWordUVRDD = dailyTop3KeyWordDF.toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {
            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                return new Tuple2<String, String>(row.getString(0), row.getString(1)
                        + "_" + String.valueOf(row.getLong(2)));
            }
        });

        JavaPairRDD<String, Iterable<String>> top3DateKeyWordUVGroupedRDD = top3DateKeyWordUVRDD.groupByKey();
        JavaPairRDD<Long, String> KUVRes = top3DateKeyWordUVGroupedRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<String, Iterable<String>> t) throws Exception {
                StringBuilder date = new StringBuilder(t._1);
                Iterable<String> keywordsAndUVs = t._2;

                Long cnt = 0L;
                Iterator<String> it = keywordsAndUVs.iterator();
                while (it.hasNext()) {
                    String kUV = it.next();
                    String[] split = kUV.split("_");
                    cnt += Long.valueOf(split[1]);
                    date.append(",");
                    date.append(kUV);
                }
                return new Tuple2<Long, String>(cnt, date.toString());
            }
        });

        //倒序排序，映射为原始格式，
        JavaRDD<Row> RESROW = KUVRes.sortByKey(false).flatMap(new FlatMapFunction<Tuple2<Long, String>, Row>() {
            @Override
            public Iterable<Row> call(Tuple2<Long, String> t) throws Exception {
                String dateAndKeyWords = t._2;//日期是一样的，都是这一天

                //日期，KUV，KUV,KUV
                String[] split = dateAndKeyWords.split(",");
                String date = split[0];

                ArrayList<Row> rows = new ArrayList<Row>();
                for (int i = 1; i <= 3; i++) {
                    String[] inner = split[i].split("_");//第i个
                    rows.add(RowFactory.create(date, inner[0], Long.valueOf(inner[1])));
                }

                return rows;
            }
        });

        //将最终的数据转换为DataFrame，并保存到hive表中
        DataFrame finalDF = hiveContext.createDataFrame(RESROW, structType);
        finalDF.saveAsTable("daily_top3_uv");

        sc.close();
    }
}
