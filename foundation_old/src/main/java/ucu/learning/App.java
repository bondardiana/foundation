package ucu.learning;

import static java.lang.String.format;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.Optional.ofNullable;
import static ucu.learning.App.Result.failure;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

import org.postgresql.util.PSQLException;

/**
 * Hello world!
 *
 */
public class App {
    private static final AtomicLong idGen = new AtomicLong(System.currentTimeMillis() * 100000);

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
        public boolean equals(final Object obj) {
            if (this == obj) {
                return true;
            }

            if (!(obj instanceof Person)) {
                return false;
            }

            final Person that = (Person) obj;

            final boolean surnameEq = this.surname != null ? this.surname.equals(that.surname) : that.surname == null;
            final boolean nameEq = this.name != null ? this.name.equals(that.name) : that.name == null;
            return surnameEq && nameEq;
        }

        @Override
        public int hashCode() {
            int result = 1;
            result = 31 * result + (this.surname != null ? this.surname.hashCode() : 0);
            result = 31 * result + (this.name != null ? this.name.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return format("Person (%s, %s, %s, %s)", id, surname, name, dob);
        }

    }

    public static class BankAccount {
        public final Long id;
        public final String number;
        public final Long owner; // id of person
        public final BigDecimal amount;
        public BankAccount(final Long id, final String number, final Long owner, final BigDecimal amount) {
            this.id = id;
            this.number = number;
            this.owner = owner;
            this.amount = amount;

        }

        @Override
        public String toString() {
            return format("BankAccount (%s, %s, %s, %s)", id, number, owner, amount);
        }
    }

    public static class Transfer {
        public final Long id;
        public final Long fromAccount; // id of the from-account
        public final Long toAccount; // id of the to-account
        public final BigDecimal amount;
        public final Date transferDate;

        public Transfer(final Long id, final Long fromAccount, final Long toAccount, final BigDecimal amount,
                final Date transferDate) {
            this.id = id;
            this.fromAccount = fromAccount;
            this.toAccount = toAccount;
            this.amount = amount;
            this.transferDate = transferDate;
        }

        @Override
        public String toString() {
            return format("Transfer (%s, %s, %s, %s, %s)", id, fromAccount, toAccount, amount, transferDate);
        }
    }

    public static Optional<Long> insertPerson(final String surname, final String name, final Date dob,
            final Connection conn) {
        final long id = idGen.incrementAndGet();
        try (final PreparedStatement ps = conn
                .prepareStatement("insert into person_ (_id, surname_, name_, dob_) values (?, ?, ?, ?)")) {
            
            ps.setLong(1, id);
            
            ps.setString(2, surname);
            ps.setString(3, name);
            ps.setDate(4, new java.sql.Date(dob.getTime()));
            if (ps.executeUpdate() != 0) {
                return of(id);
            }
        } catch (final Exception ex) {
            ex.printStackTrace();
        }

        return empty();
    }

    public static Optional<Long> insertBankAccount(final String number, final Long owner, final BigDecimal amount,
            final Connection conn) {
        final long id = idGen.incrementAndGet();

        try (final PreparedStatement ps = conn
                .prepareStatement("insert into bankaccount_ (_id, number_, owner_, amount_) values (?, ?, ?, ?)")) {
            ps.setLong(1, id);
            ps.setString(2, number);
            ps.setLong(3, owner);
            ps.setBigDecimal(4, amount);
            if (ps.executeUpdate() != 0) {
                return of(id);
            }
        } catch (final Exception ex) {
            ex.printStackTrace();
        }

        return empty();
    }

    public static Optional<Long> insertTransfer(final Long fromAccount, final Long toAccount, final BigDecimal amount,
            final Date transferDate, final Connection conn) {
        final long id = idGen.incrementAndGet();

        try (final PreparedStatement ps = conn.prepareStatement(
                "insert into transfer_ (_id, fromaccount_, toaccount_, amount_, transferdate_) values (?, ?, ?, ?, ?)")) {
            ps.setLong(1, id);
            ps.setLong(2, fromAccount);
            ps.setLong(3, toAccount);
            ps.setBigDecimal(4, amount);
            ps.setTimestamp(5, new java.sql.Timestamp(transferDate.getTime()));
            if (ps.executeUpdate() != 0) {
                return of(id);
            }
        } catch (final Exception ex) {
            ex.printStackTrace();
        }

        return empty();
    }

    public static List<Person> allPersons(final Connection conn) {
        final var persons = new ArrayList<Person>();
        try (final Statement st = conn.createStatement();
                final ResultSet rs = st.executeQuery("select * from person_")) {
            while (rs.next()) {
                var p = new Person(rs.getLong("_id"), rs.getString("surname_"), rs.getString("name_"),
                        rs.getDate("dob_"));
                persons.add(p);
            }
        } catch (final Exception ex) {
            ex.printStackTrace();
        }

        return persons;
    }

    public static List<BankAccount> allBankAccounts(final Connection conn) {
        final var bankAccounts = new ArrayList<BankAccount>();
        try (final Statement st = conn.createStatement();
                final ResultSet rs = st.executeQuery("select * from bankaccount_")) {
            while (rs.next()) {
                var ba = new BankAccount(rs.getLong("_id"), rs.getString("number_"), rs.getLong("owner_"),
                        rs.getBigDecimal("amount_"));
                bankAccounts.add(ba);
            }
        } catch (final Exception ex) {
            ex.printStackTrace();
        }

        return bankAccounts;
    }

    public static List<Transfer> allTransfers(final Connection conn) {
        final var transfers = new ArrayList<Transfer>();
        try (final Statement st = conn.createStatement();
                final ResultSet rs = st.executeQuery("select * from transfer_")) {
            while (rs.next()) {
                var tr = new Transfer(rs.getLong("_id"), rs.getLong("fromaccount_"), rs.getLong("toaccount_"),
                        rs.getBigDecimal("amount_"), rs.getTimestamp("transferdate_"));
                transfers.add(tr);
            }
        } catch (final Exception ex) {
            ex.printStackTrace();
        }

        return transfers;
    }

    public static int updateAmountOnAccount(final Long account, final BigDecimal amount, final Connection conn) {
        try (final PreparedStatement ps = conn.prepareStatement("update bankaccount_ set amount_ = ? where _id = ?")) {
            ps.setBigDecimal(1, amount);
            ps.setLong(2, account);
            return ps.executeUpdate();
        } catch (final Exception ex) {
            ex.printStackTrace();
            return 0;
        }
    }

    public static Optional<BigDecimal> bankAccountAmount(final Long account, final Connection conn) {
        try (final PreparedStatement ps = conn.prepareStatement("select amount_ from bankaccount_ where _id = ?")) {
            ps.setLong(1, account);
            try (final ResultSet rs = ps.executeQuery()) {
                return rs.next() ? of(rs.getBigDecimal("amount_")) : empty();
            }
        } catch (final Exception ex) {
            ex.printStackTrace();
            return empty();
        }
    }

   
    public static List<Person> findPersonBySurname(final String surname, final Connection conn) {
        final var person = new ArrayList<Person>();
        try (final PreparedStatement ps = conn.prepareStatement("select * from person_ where surname_ = ?")) {
            ps.setString(1, surname);
            try (final ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    var p = new Person(rs.getLong("_id"), rs.getString("surname_"), rs.getString("name_"),
                            rs.getDate("dob_"));
                    person.add(p);

                }

            }
        } catch (final Exception ex) {
            ex.printStackTrace();
        }

        return person;

    }

    public static int deleteAllPersonnel(final Connection conn) {
        deleteAllBankAccounts(conn);
        try (final PreparedStatement ps = conn.prepareStatement("delete from person_")) {
            return ps.executeUpdate();
        } catch (final Exception ex) {
            ex.printStackTrace();
            return 0;
        }
    }

    public static int deleteAllBankAccounts(final Connection conn) {
        deleteAllTrasfers(conn);
        try (final PreparedStatement ps = conn.prepareStatement("delete from bankaccount_")) {
            return ps.executeUpdate();
        } catch (final Exception ex) {
            ex.printStackTrace();
            return 0;
        }
    }

    public static int deleteAllTrasfers(final Connection conn) {
        try (final PreparedStatement ps = conn.prepareStatement("delete from transfer_")) {
            return ps.executeUpdate();
        } catch (final Exception ex) {
            ex.printStackTrace();
            return 0;
        }
    }

    public static void printId(final Long id) {
        System.out.println("ID:" + id);
    }

    public static Optional<Date> mkDate(final int year, final int month, final int day) {
        try {
            final LocalDate localDate = LocalDate.of(year, month, day);
            return of(Date.from(localDate.atStartOfDay(ZoneId.systemDefault()).toInstant()));
        } catch (final Exception ex) {
            System.out.println(ex);
            return empty();
        }
    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException , PSQLException {
        System.out.println("Example runner.\n");
        final Optional<Date> dob1 = mkDate(2020, 2, 28);
        final Optional<Date> dob2 = mkDate(2001, 1, 11);
        final Optional<Date> d1 = mkDate(2021, 1, 11);
        
        final String dbUrl = "jdbc:postgresql://localhost:5432/foundation";
        try (final Connection conn =  DriverManager.getConnection(dbUrl, "dbuser", "dbpassw0rd");) {
            conn.setAutoCommit(true);
            
            System.out.printf("The number of personnel records deleted: %s%n", deleteAllPersonnel(conn));
        
            // creating persons 
            dob1.ifPresent(date -> insertPerson("Alonzo", "Church", date, conn).ifPresent(App::printId));
            dob2.ifPresent(date -> insertPerson("Diana", "Bondar", date, conn).ifPresent(App::printId));
            
            // create persons with amounts 
            final Optional<Long> account1 = mkDate(1905, 12, 9)
                    .flatMap(dob -> insertPerson("Di", "didi", dob, conn))
                    .flatMap(owner -> insertBankAccount("123", owner, new BigDecimal("26.00"), conn));
            final Optional<Long> account2 = mkDate(1939, 11, 7)
                    .flatMap(dob -> insertPerson("didi", "di", dob, conn))
                    .flatMap(owner -> insertBankAccount("1234", owner, new BigDecimal("77.00"), conn));
            
            allBankAccounts(conn).forEach(System.out::println);
            updateAmountOnAccount(account1.get(), new  BigDecimal(10.00), conn);
            
            System.out.println("Here it works");
            allBankAccounts(conn).forEach(System.out::println);
           
            
            System.out.println("transfer insert step");
            if (account1.isPresent() && account2.isPresent()) {
                d1.ifPresent(date -> insertTransfer(account1.get(), account2.get(), new BigDecimal("15.00"), date, conn).ifPresent(System.out::println));
            }
            allTransfers(conn).forEach(System.out::println);
            
            System.out.println("transfer delete step ");
            System.out.printf("The number of transfers  deleted: %s%n", deleteAllTrasfers(conn));
            
            allTransfers(conn).forEach(System.out::println);
     

        }
    }

    private static Iterable<Throwable> allAccount(Connection conn) {
        // TODO Auto-generated method stub
        return null;
    }
}