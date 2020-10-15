package ucu.learning;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws ClassNotFoundException, SQLException
    {
        System.out.println( "Hello World!" );

        final String dbUrl = "jdbc:postgresql://localhost:5432/foundation";
        try (final Connection conn = DriverManager.getConnection(dbUrl, "dbuser", "dbpassw0rd");
             final ResultSet rs = conn.createStatement().executeQuery("select * from person_");) {
            while (rs.next()) {
                System.out.printf("Person: %s %s%n", rs.getString("name_"), rs.getString("surname_")); 
            }
        }
    }
}
