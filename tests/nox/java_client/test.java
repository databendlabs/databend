import java.sql.SQLException;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;

public class Main {
    public static void main(String[] args) throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:databend://localhost:8000", "root", "");
        Statement statement = conn.createStatement();
        statement.execute("SELECT number from numbers(5) order by number");
        ResultSet rs = statement.getResultSet();
        // ** We must call `rs.next()` otherwise the query may be canceled **
        while (rs.next()) {
            System.out.println(rs.getInt(1));
        }
        conn.close();
    }
}