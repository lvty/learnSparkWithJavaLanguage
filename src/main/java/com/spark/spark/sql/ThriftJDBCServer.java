import java.sql.*;

public class ThriftJDBCServer {
    public static void main(String[] args) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        String sql = "select * from top3_sales where revenue =?";
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");

            connection = DriverManager.getConnection(
                    "jdbc:hive2://node1:10001/default?" +
                            "hive.server2.transport.mode=http;" +
                            "hive.server2.thrift.http.path=cliservice",
                    "root",
                    "");
            ps = connection.prepareStatement(sql);
            ps.setInt(1,6500);
            rs = ps.executeQuery();
            while(rs.next()){
                System.out.println(rs.getInt(3));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if(ps != null){
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            if(connection != null){
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

    }
}
