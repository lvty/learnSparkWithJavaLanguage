
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.List;

public class JSONLoadData {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        DataFrame jsonDF = sqlContext.read().json("F:\\idea\\learnSparkWithJavaLanguage\\src\\data\\students.json");

        //注册临时表，查询分数大于80分的学生的姓名
        jsonDF.registerTempTable("student_scores");
        DataFrame nameDF = sqlContext.sql("select name,score from student_scores where score >= 80");

        List<String> goodStudentNames = nameDF.javaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                return row.getString(0);
            }
        }).collect();

        //然后针对JavaRDD创建DataFrame
        List<String> studentClassInfos = new ArrayList<String>();
        studentClassInfos.add("{\"name\":\"leo\",\"class\":\"Class_1\"}");
        studentClassInfos.add("{\"name\":\"marry\",\"class\":\"Class_1\"}");
        studentClassInfos.add("{\"name\":\"jack\",\"class\":\"Class_2\"}");


        JavaRDD<String> studentsClassInfosRDD = sc.parallelize(studentClassInfos,2);
        DataFrame studentsClassInfosDf = sqlContext.read().json(studentsClassInfosRDD);
        studentsClassInfosDf.registerTempTable("studentsClassInfos");

        //拼接SQL语句
        StringBuilder sb = new StringBuilder();
        sb.append("select name,class from studentsClassInfos where name in(");
        for (int i = 0; i < goodStudentNames.size(); i++) {
            sb.append("'");
            sb.append(goodStudentNames.get(i));
            sb.append("'");
            if(i < goodStudentNames.size() - 1){
                sb.append(",");
            }
        }
        sb.append(")");

        DataFrame resDF = sqlContext.sql(sb.toString());


        //根据name join两份RDD的数据到一起
        JavaRDD<Row> mapRowResRDD = nameDF.javaRDD().mapToPair(new PairFunction<Row, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Row row) throws Exception {
                return new Tuple2<String, Long>(row.getString(0), row.getLong(1));
            }
        }).join(resDF.toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {
            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                return new Tuple2<String, String>(row.getString(0), row.getString(1));
            }
        })).map(new Function<Tuple2<String, Tuple2<Long, String>>, Row>() {
            @Override
            public Row call(Tuple2<String, Tuple2<Long, String>> t) throws Exception {
                //使用编程方式，将RDD转换为DataFrame
                return RowFactory.create(t._1, t._2._1, t._2._2);
            }
        });

        ArrayList<StructField> fields = new ArrayList<StructField>();
        fields.add(DataTypes.createStructField("name", DataTypes.StringType,true));
        fields.add(DataTypes.createStructField("score",DataTypes.LongType,true));
        fields.add(DataTypes.createStructField("class",DataTypes.StringType,true));

        StructType structType = DataTypes.createStructType(fields);

        //转换生成RDD
        DataFrame getResDF = sqlContext.createDataFrame(mapRowResRDD, structType);

        //getResDF.registerTempTable("res");
        //sqlContext.sql("select *from res").show();
        //结果保存至一个json文件中
        getResDF.write().json("F:\\idea\\learnSparkWithJavaLanguage\\src\\data\\Res");

        sc.close();

    }
}
