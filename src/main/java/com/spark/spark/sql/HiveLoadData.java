
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

/**
 * hive 数据源
 */
public class HiveLoadData {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("HiveDataSource");
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext hiveContext = new HiveContext(sc.sc());//使用的是sparkcont创建的HiveContext

        //将学生基本信息表的数据导入student_infos表

        //第一个功能，使用hivecontext的sql()/hql()方法，可以执行hive中能够执行的hiveql语句
        hiveContext.sql("drop table if exists student_infos");//判断表student_infos表若存在就直接删除
        //判断不存在则创建
        hiveContext.sql("create table if not exists student_infos (name String,age int)");
        //将学生信息导入student_infos表
        hiveContext.sql("load data local inpath '/root/sparkstudy/java/resource/student_infos.txt'" +
                                " into table student_infos");

        //同样的方式导入student_scores
        hiveContext.sql("drop table if exists student_scores");
        hiveContext.sql("create table if not exists student_scores (name String,score int)");
        hiveContext.sql("load data local inpath '/root/sparkstudy/java/resource/student_scores.txt' " +
                                "into table student_scores");

        //执行SQL查询，关联两张表，查询成绩大于80分的学生信息
        DataFrame res = hiveContext.sql("select s1.name,s1.age,s2.score " +
                "from student_infos s1  " +
                "join student_scores s2  " +
                "on s1.name = s2.name " +
                "where s2.score >= 80 ");

        //接着讲DataFrame中的数据保存到good_student_infos表中
        hiveContext.sql("drop table if exists good_student_infos");
        res.saveAsTable("good_student_infos");

        //针对good_student_infos表，可以直接创建DataFrame
        Row[] good_student_infosRows = hiveContext.table("good_student_infos").collect();
        for(Row r:good_student_infosRows){
            System.out.println(r);
        }

        sc.close();
    }
}
