
package ucu.learning;

import static java.lang.String.format;
import static java.util.Optional.empty;
import static java.util.Optional.of;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Hello world!
 *
 */
public class App {
    
    private static final AtomicLong idGen = new AtomicLong (System.currentTimeMillis()*10000);
    
    public static class Person {
        public final Long id;
        public final String surname;
        public final String name;
        public final Date dob;

        public Person(final Long id, final String surname, final String name, final Date dob) {
            this.id = id;
            this.surname = surname;
            this.name = name;
            this.dob = dob;
        }

        @Override
        public String toString() {
            return format("Person ( %s, %s, %s, %s )", id, surname, name, dob);
        }
    }


    public static List<Person> allPersons(final Connection conn) {
        final var persons = new ArrayList<Person>();
        try (final Statement st = conn.createStatement();
                final ResultSet rs = st.executeQuery("select * from person_")) {
            while (rs.next()) {
                var p = new Person(rs.getLong("_id"), rs.getString("surname_"), rs.getString("name_"), rs.getDate("dob_"));
                persons.add(p);
            }
        } catch (final Exception ex) {
            ex.printStackTrace();
        }
        return persons;
    }

    public static List<Person> findPersonBySurname(final String surname, final Connection conn) {
        final var persons = new ArrayList<Person>();
        try (final PreparedStatement ps = conn.prepareStatement("select * from person_ where surname_ = ?")) {
            ps.setString(1, surname);
            try (final ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    var p = new Person(rs.getLong("_id"), rs.getString("surname_"), rs.getString("name_"), rs.getDate("dob_"));
                    persons.add(p);
                }
            }
        } catch (final Exception ex) {
            ex.printStackTrace();
        }
        return persons;
    }
    
    public static int deleteAllPersonnel(final Connection conn) {
        try (final PreparedStatement ps = conn.prepareStatement("delete from person_ ")){
            return ps.executeUpdate();
        } catch (final Exception ex) {
            ex.printStackTrace();
            return 0;
        }
    }
    
    public static Long insertPerson(final String surname, final String name, final Date dob, final Connection conn) {
        final long id = idGen.incrementAndGet();
        
        try (final PreparedStatement ps = conn.prepareStatement("insert into person_ ( name_ , surname_ , dob_ ) values(?,?, ?);")) {
            //ps.setLong(1, 3);
            ps.setString(1, surname);
            ps.setString(2, name);
            ps.setDate(3, new java.sql.Date(dob.getTime()));
            ps.execute();
            
        } catch (final Exception ex) {
            ex.printStackTrace();
        }
        
        return id;
        
    }
    
    public static void printId (final Long id) {
        System.out.println("ID:" + id);
    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException, ParseException {
        System.out.println("Hello World!");
        //Class.forName("org.postgressql.Driver");
        
        SimpleDateFormat parser = new SimpleDateFormat("yyyy-mm-dd");
        
        
        final String dbUrl = "jdbc:postgresql://localhost:5432/foundation";
        try (final Connection conn = DriverManager.getConnection(dbUrl, "dbuser", "dbpassw0rd")) {

          
            
            allPersons(conn).forEach(System.out::println);
            System.out.println(insertPerson("Anna", "Maria", parser.parse("1994-05-07"), conn));
            allPersons(conn).forEach(System.out::println);

            
         
        }
    }

}