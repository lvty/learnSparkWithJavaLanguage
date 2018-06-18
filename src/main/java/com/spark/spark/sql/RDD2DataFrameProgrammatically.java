
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import java.util.ArrayList;
import java.util.List;

public class RDD2DataFrameProgrammatically {
    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setMaster("local").setAppName(""));
        SQLContext sqlContext = new SQLContext(sc);

        //第一步，创建一个普通的RDD，但是必须将其转换为RDD<Row>的这种格式
        JavaRDD<String> lines = sc.textFile("F:\\idea\\learnSparkWithJavaLanguage\\src\\data\\students.txt", 2);
        JavaRDD<Row> mapRowRDD = lines.map(new Function<String, Row>() {
            @Override
            public Row call(String s) throws Exception {
                String[] split = s.split(",");
                return RowFactory.create(Integer.valueOf(split[0]), split[1], Integer.valueOf(split[2]));
            }
        });

        //第二步，动态构造元数据,比如说id、name等field的名称和类型，可能都是在程序运行过程中，动态的从mysql DB里或者
        //配置文件里加载出来的，是不固定的 ，所以特别适合用这种编程的方式来狗仔元数据
        ArrayList<StructField> fields = new ArrayList<StructField>();
        fields.add(DataTypes.createStructField("id", DataTypes.IntegerType,false));
        fields.add(DataTypes.createStructField("name", DataTypes.StringType,false));
        fields.add(DataTypes.createStructField("age",DataTypes.IntegerType,false));

        StructType structType = DataTypes.createStructType(fields);

        //第三步，使用动态构造元数据，将RDD转换为DataFrame
        DataFrame df = sqlContext.createDataFrame(mapRowRDD, structType);

        df.registerTempTable("student");
        DataFrame res = sqlContext.sql("select * from student where age <= 18");
        List<Row> rs = res.javaRDD().collect();
        for(Row r :rs){
            System.out.println(r);
        }

        /*//定义structType
        val schema = StructType(
                Array(
                        StructField("id", IntegerType, true),
                        StructField("name", StringType, true),
                        StructField("age", IntegerType, true)
                )
        )*/


        sc.close();
    }
}
