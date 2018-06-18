
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

//将RDD转换为DataFrame
public class RDD2DataFrameReflection {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        JavaRDD<String> rdd = sc.textFile("F:\\idea\\learnSparkWithJavaLanguage\\src\\data\\students.txt", 2);
        //将每行转换为studentRDD
        JavaRDD<Student> studentJavaRDD = rdd.map(new Function<String, Student>() {
            @Override
            public Student call(String s) throws Exception {
                String[] split = s.split(",");
                return new Student(Integer.valueOf(split[0]), split[1], Integer.valueOf(split[2]));
            }
        });

        //使用反射方式，将RDD转换为DataFrame
        //将Student.class传入进去，其实就是用反射的方式来创建DataFrame；因为student.class本身就是反射的一个应用
        //然后底层还要通过Student class进行反射 来获取其中的field
        DataFrame df = sqlContext.createDataFrame(studentJavaRDD, Student.class);

        //df.printSchema();
        df.registerTempTable("student");
        DataFrame res = sqlContext.sql("select * from student where age <= 18");
        JavaRDD<Row> resJavaRDD = res.javaRDD();//DataFrame再次转换为RDD

        //将RDD的数据进行映射为student

        JavaRDD<Student> resStudentRDD = resJavaRDD.map(new Function<Row, Student>() {
            @Override
            public Student call(Row row) throws Exception {
                int id = row.getInt(1);
                String name = row.getString(2);
                int age = row.getInt(0);
                return new Student(id,name,age);
            }
        });

        List<Student> collect = resStudentRDD.collect();
        for (Student s:collect){
            System.out.println(s);
        }
        sc.close();
    }


}
