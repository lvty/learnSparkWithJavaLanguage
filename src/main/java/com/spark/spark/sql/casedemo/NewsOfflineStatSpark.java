package com.spark.spark.sql.casedemo;

import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class NewsOfflineStatSpark {
    public static void main(String[] args) {
        //创建上下文
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        HiveContext hiveContext = new HiveContext(jsc);

        //开发第一个关键指标：页面pv统计以及排序，针对昨日数据，执行SQL语句

        //一般来说，每次spark作业计算出来的结果，实际上，大部分情况下，都会写入mysql等存储，这样的话，
        //我们可以基于mysql使用javaweb系统开发一套系统平台，来使用图表的凡是展示每次spark计算出来的关键指标
        //比如折线图，可以反映最近一周的每天的用户跳出率的变化；也可以通过页面，给用户提供最近一个查询表单，
        //可以查询指定的页面最近一周的PV变化
        //比如 date pageid pv 插入到mysql中，后面用户就可以查询指定日期段内的某一个page对应的所有pv，然后
        //使用折线图来反映变化曲线
        String yesterday = getYesterday();
        System.out.println("=============Pv:=============");
        calculateDailyPagePv(hiveContext,yesterday);
        System.out.println("=============Uv:=============");
        calculateDailyPageUv(hiveContext,yesterday);
        System.out.println("=============新用户注册率：=====" + calculateDailyNewUserRegisterRate(hiveContext, yesterday));

        //用户跳出率统计
        System.out.println("=============跳出率：=========="+calculateUserJumpRate(hiveContext,yesterday));

        //版块热度排行榜，根据每个版块每天被访问的次数，做出一个排行榜
        System.out.println("==========热榜排行榜===========");
        calculateDailySectionHotSort(hiveContext,yesterday);

        jsc.close();
    }

    private static void calculateDailySectionHotSort(HiveContext hiveContext, String yesterday) {
        String sql = "select a_date,section,pv from( " +
                            "select a_date,section,count(*) pv from news_access where a_date = '" + yesterday + "'" +
                            "and action = 'view' group by section,a_date " +
                      ") t order by pv desc";
        DataFrame df = hiveContext.sql(sql);

        //可以转换为RDD，写入mysql中
        df.show();
    }

    //计算已注册用户每天的用户跳出率
    private static double calculateUserJumpRate(HiveContext hiveContext, String yesterday) {
        String sql1 = "select count(*) from news_access where action = 'view' and a_date = '"+ yesterday +"' and userid is not null";
        //跳出用户
        String sql2 = "select count(*) from (" +
                "select count(*) cnt " +
                "from news_access where action = 'view' " +
                "and a_date = '"+ yesterday +"'" +
                "and userid is not null group by userid having cnt == 1 ) t";
        //执行，获取结果
        Long totalNumberObj = Long.valueOf(String.valueOf(hiveContext.sql(sql1).collect()[0].get(0)));
        Long jumpNumberObj = Long.valueOf(String.valueOf(hiveContext.sql(sql2).collect()[0].get(0)));

        //计算结果
        if(totalNumberObj == null || jumpNumberObj == null){
            throw new RuntimeException("compute error");
        }
        return (double)jumpNumberObj/(double)totalNumberObj;
    }

    //计算每天新注册的用户比例
    private static double calculateDailyNewUserRegisterRate(HiveContext hiveContext, String yesterday) {
        //获取的是分母，当天未注册用户的访问数
        String sql1 = "select count(*) from news_access where action = 'view' and a_date = '"+ yesterday +"' and userid is null";
        String sql2 = "select count(*) from news_access where action = 'register' and a_date = '"+ yesterday +"'";

        //执行，获取结果
        Long totalUnregisterNumberObj = Long.valueOf(String.valueOf(hiveContext.sql(sql1).collect()[0].get(0)));
        Long totalRegisterNumberObj = Long.valueOf(String.valueOf(hiveContext.sql(sql2).collect()[0].get(0)));

        //计算结果
        if(totalRegisterNumberObj == null || totalUnregisterNumberObj == null){
            throw new RuntimeException("compute error");
        }
        return (double)totalRegisterNumberObj/(double)totalUnregisterNumberObj;
    }

    //计算每天每个页面的pv，并排序，排序的好处是，结果插入到mysql之后就可以直接基于limit10 计算，
    //否则需要java web系统做排序，影响了web系统的响应时间
    private static void calculateDailyPagePv(HiveContext hiveContext,String d){
        String sql = "select " +
                "a_date,pageid,pv " +
                "from " +
                "(select " +
                "a_date,pageid,count(*) pv from news_access where a_date = '"+ d +"'" +
                " group by a_date,pageid" +
                ") t order by pv desc";
        DataFrame df = hiveContext.sql(sql);

        //可以转换为RDD，写入mysql中
        df.show();

    }

    //计算每天每个页面的UV以及排序,spark sql的count(distinct)语句，是由bug的，默认会差生严重的数据倾斜问题，只会使用一个task，去做去重和汇总计算，性能很差
    private static void calculateDailyPageUv(HiveContext hiveContext,String d){
        String sql =
                "select " +
                        "a_date,pageid,uv " +
                "from " +
                "( " +
                    "select " +
                    "    a_date,pageid,count(1) uv " +
                    "from ( " +
                    "      select " +
                    "           a_date,pageid,userid " +
                    "      from news_access " +
                    "      where action = 'view' " +
                    "      and a_date = '"+ d +"'" +
                    "      group by a_date,pageid,userid " +
                    ") t2 " +
                    "group by a_date,pageid " +
                ") t " +
                "order by uv desc";
        DataFrame df = hiveContext.sql(sql);

        //可以转换为RDD，写入mysql中
        df.show();

    }

    //获取昨日的字符串日期
    public static String getYesterday(){
        Calendar instance = Calendar.getInstance();
        instance.setTime(new Date());
        instance.add(Calendar.DAY_OF_YEAR,-1);
        Date yest = instance.getTime();

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        return simpleDateFormat.format(yest);
    }
}
