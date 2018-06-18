
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;

public class JDBCLoadData {
    public static void main(String[] args) {
        final SparkConf conf = new SparkConf().setAppName("");
        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc.sc());
        HashMap<String, String> map = new HashMap<String, String>();
        map.put("url","jdbc:mysql://node1:3306/mydb");
        map.put("dbtable","student_infos");
        DataFrame students_infos = sqlContext.read().format("jdbc").options(map).load();


        map.put("dbtable","student_scores");
        DataFrame student_scores = sqlContext.read().format("jdbc").options(map).load();


        //将两个DataFrame转换为javapairrdd，执行join操作
        JavaRDD<Row> resMapRDD = students_infos.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<String, Integer>(row.getString(0), Integer.valueOf(String.valueOf(row.getInt(1))));
            }
        }).join(student_scores.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<String, Integer>(row.getString(0), Integer.valueOf(String.valueOf(row.getInt(1))));
            }
        })).map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Row>() {
            @Override
            public Row call(Tuple2<String, Tuple2<Integer, Integer>> t) throws Exception {
                return RowFactory.create(t._1, t._2._1, t._2._2);
            }
        });

        //过滤操作
        JavaRDD<Row> filterResMapRDD = resMapRDD.filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row row) throws Exception {
                return Integer.valueOf(String.valueOf(row.get(2))) >= 80;
            }
        });

        ArrayList<StructField> fields = new ArrayList<StructField>();
        fields.add(DataTypes.createStructField("name", DataTypes.StringType,true));
        fields.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));
        fields.add(DataTypes.createStructField("score", DataTypes.IntegerType,true));

        StructType structType = DataTypes.createStructType(fields);
        DataFrame resDF = sqlContext.createDataFrame(filterResMapRDD, structType);
        System.out.println(resDF.collect());

        //将DataFrame的数据保存到mysql表中
        resDF.javaRDD().foreach(new VoidFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                Class.forName("com.mysql.jdbc.Driver");
                String url = "jdbc:mysql://node1:3306/mydb";
                String user = "";
                String pwd = "";
                Connection connection = null;
                PreparedStatement stmt = null;
                String sql = "INSERT INTO good_students_infos VALUES (?,?,?)";
                try {
                    connection= DriverManager.getConnection(url, user, pwd);
                    stmt = connection.prepareStatement(sql);
                    stmt.setString(1,row.getString(0));
                    stmt.setInt(2,row.getInt(1));
                    stmt.setInt(3,row.getInt(2));
                    stmt.executeUpdate();

                }catch (Exception e){
                    throw new RuntimeException("connection error");
                }finally {
                    if(stmt != null){
                        stmt.close();
                    }
                    if(connection != null){
                        connection.close();
                    }
                }
            }
        });

        sc.close();
    }
}
