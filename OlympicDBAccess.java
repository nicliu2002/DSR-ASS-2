import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.Date;

public class OlympicDBAccess {

    Connection con;

    public OlympicDBAccess(){ //opens connection to the database
        try
        {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
        }
        catch (Exception ex)
        {
            System.err.println("Unable to load MySQL driver.");
            ex.printStackTrace();
        }
    }

    public void createTables() {
        Statement stmt = null;
        try {
            con = DriverManager.getConnection("jdbc:mysql://seitux2.adfa.unsw.edu.au/z5364371", "z5364371", "TapHolster64");
            stmt = con.createStatement();
            stmt.executeUpdate("CREATE TABLE OLYMPICS ("
                    + "ID INT NOT NULL AUTO_INCREMENT,"
                    + "year INT,"
                    + "season VARCHAR(7),"
                    + "city VARCHAR(23),"
                    + "PRIMARY KEY (ID))");
            stmt.executeUpdate("CREATE TABLE EVENTS ("
                    + "ID INT NOT NULL AUTO_INCREMENT,"
                    + "sport VARCHAR(26),"
                    + "event VARCHAR(86),"
                    + "PRIMARY KEY (ID))");
            stmt.executeUpdate("CREATE TABLE ATHLETES ("
                    + "ID INT NOT NULL AUTO_INCREMENT,"
                    + "name VARCHAR(94),"
                    + "noc CHAR(3),"
                    + "gender CHAR(1),"
                    + "PRIMARY KEY (ID))");
            stmt.executeUpdate("CREATE TABLE MEDALS ("
                    + "ID INT NOT NULL AUTO_INCREMENT,"
                    + "olympicID INT,"
                    + "eventID INT,"
                    + "athleteID INT,"
                    + "PRIMARY KEY (ID),"
                    + "FOREIGN KEY (olympicID) REFERENCES OLYMPICS(ID),"
                    + "FOREIGN KEY (eventID) REFERENCES EVENTS(ID),"
                    + "FOREIGN KEY (athleteID) REFERENCES ATHLETES(ID))");
            con.commit();
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void dropTables() {
        Statement stmt = null;
        try {
            con = DriverManager.getConnection("jdbc:mysql://seitux2.adfa.unsw.edu.au/z5364371", "z5364371", "TapHolster64");
            stmt = con.createStatement();
            stmt.executeUpdate("DROP TABLE MEDALS"); //drops medals first to avoid foreign key problems
            stmt.executeUpdate("DROP TABLE ATHLETES");
            stmt.executeUpdate("DROP TABLE EVENTS");
            stmt.executeUpdate("DROP TABLE OLYMPICS");
            con.commit();
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public void populateTables() {
        //this should be the first line in this method.
        long time = System.currentTimeMillis();
        populateOlympics();
        populateEvents();
        populateAthletes();

        //this should be the last line in this method
        System.out.println("Time to populate: " + (System.currentTimeMillis() - time) + "ms");
    }

    public void populateOlympics (){
        int batchSize = 20; //adjust based on imported csv , roughly 20 is most efficient for this method
        try {
            con = DriverManager.getConnection("jdbc:mysql://seitux2.adfa.unsw.edu.au/z5364371", "z5364371", "TapHolster64");
            String sql = "INSERT INTO OLYMPICS (year, season, city) VALUES (?, ?, ?)";
            PreparedStatement statement = con.prepareStatement(sql);
            BufferedReader lineReader = new BufferedReader(new FileReader("olympics.csv"));
            String lineText = null;
            int count = 0;
            lineReader.readLine(); // skip header line
            while ((lineText = lineReader.readLine()) != null) {
                count++;
                String[] data = lineText.split(",");
                String year = data[0];
                String season = data[1];
                String city = data[2];
                int intYear = Integer.parseInt(year);
                statement.setInt(1, intYear);
                statement.setString(2, season);
                statement.setString(3, city);
                statement.addBatch();
                if (count % batchSize == 0) {
                    statement.executeBatch();
                }
            }
            lineReader.close();
            // execute the remaining queries
            statement.executeBatch();
            con.commit();
            con.close();
        } catch (IOException ex) {
            System.err.println(ex);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }
    public void populateEvents() {
        int batchSize = 20; //adjust based on imported csv , roughly 20 is most efficient for this method
        try {
            con = DriverManager.getConnection("jdbc:mysql://seitux2.adfa.unsw.edu.au/z5364371", "z5364371", "TapHolster64");
            String sql = "INSERT INTO EVENTS (sport, event) VALUES (?, ?)";
            PreparedStatement statement = con.prepareStatement(sql);
            BufferedReader lineReader = new BufferedReader(new FileReader("events.csv"));
            String lineText = null;
            int count = 0;
            lineReader.readLine(); // skip header line
            while ((lineText = lineReader.readLine()) != null) {
                count++;
                String[] data = lineText.split(",");
                String sport = data[0];
                String event = data[1];
                statement.setString(1, sport);
                statement.setString(2, event);
                statement.addBatch();
                if (count % batchSize == 0) {
                    statement.executeBatch();
                }
            }
            lineReader.close();
            // execute the remaining queries
            statement.executeBatch();
            con.commit();
            con.close();
        } catch (IOException ex) {
            System.err.println(ex);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }
    public void populateAthletes() {
        int batchSize = 20; //adjust based on imported csv , roughly 20 is most efficient for this method
        try {
            con = DriverManager.getConnection("jdbc:mysql://seitux2.adfa.unsw.edu.au/z5364371", "z5364371", "TapHolster64");
            String sql = "INSERT INTO ATHLETES (name, noc, gender) VALUES (?, ?, ?)";
            PreparedStatement statement = con.prepareStatement(sql);
            BufferedReader lineReader = new BufferedReader(new FileReader("events.csv"));
            String lineText = null;
            int count = 0;
            lineReader.readLine(); // skip header line
            while ((lineText = lineReader.readLine()) != null) {
                count++;
                String[] data = lineText.split(",");
                String name = data[0];
                String noc = data[1];
                String gender = data[2];
                statement.setString(1, name);
                statement.setString(2, noc);
                statement.setString(3, gender);
                statement.addBatch();
                if (count % batchSize == 0) {
                    statement.executeBatch();
                }
            }
            lineReader.close();
            // execute the remaining queries
            statement.executeBatch();
            con.commit();
            con.close();
        } catch (IOException ex) {
            System.err.println(ex);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }


        public void runQueries() {

    }


}

