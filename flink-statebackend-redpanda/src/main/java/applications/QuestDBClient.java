import java.sql.*;
import java.util.Properties;

public class QuestDBClient {

    Connection connection;
    PreparedStatement query;

    public QuestDBClient(String query_) {
        try{
            Properties properties = new Properties();
            properties.setProperty("user", "admin");
            properties.setProperty("password", "quest");
            properties.setProperty("sslmode", "disable");

            this.connection = DriverManager.getConnection(
                "jdbc:postgresql://localhost:8812/qdb", properties);

            this.query = this.connection.prepareStatement(
                query_
            );
        }
        catch(Exception e){
        }
    }

    public void query_latest_timestamp(){
        try (ResultSet rs = this.query.executeQuery()) {
            while (rs.next()) {
                System.out.println(rs.getTimestamp("ts"));
            }
        }
        catch(Exception e){}
    }

    public void close(){
        try {
            this.connection.close();
        } catch (Exception e) {
            //TODO: handle exception
        }
    }

    public static void main(String... args) throws Exception {

        String query = (
            "SELECT timestamp\n"+
            "FROM wikitable\n"+
            "ORDER BY timestamp DESC\n"+
            "LIMIT 1;");
        QuestDBClient client = new QuestDBClient(query);
       
        // TODO: query while the table is still being updated
        client.query_latest_timestamp();

        client.close();
    }
}
