

 /*

The procedure for callableStatement that returned a ResulSet was:

delimiter //

DROP PROCEDURE IF EXISTS `sakila`.`simpleproc` //


CREATE  PROCEDURE  simpleproc ()
	BEGIN
		SELECT * FROM actor;
	END//	
        
delimiter ;

 */
// Requirements


 */
package databasetest;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
//import java.sql.Date;
import java.util.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import static java.time.Instant.now;
import static java.time.LocalDate.now;
import static java.time.LocalDate.now;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author MMA
 */
public class DatabaseTest {

    /**
     * @param args the command line arguments
     */
    public static ResultSetMetaData rsmd;
    public static Connection conn;
    public static ArrayList<String> fileData;

    public static void main(String[] args) {
        // TODO code application logic here

        // read file
        
        fileData = new ArrayList<String>();
        
        File file = new File("GeoLiteCity-Location.csv");
        FileReader fileReader;
        try {
            fileReader = new FileReader(file);
            BufferedReader br = new BufferedReader(fileReader);
            
            StringBuffer stringBuffer = new StringBuffer();
            String line;
            
            try {
                while((line = br.readLine()) != null){
                    
                    stringBuffer.append(line);
                    stringBuffer.append("\n");
                    fileData.add(line);
                    
                }
                
                fileReader.close();
                System.out.println("Contents: ");
                //System.out.println(stringBuffer.toString());
                
//                for (int i = 2; i < fileData.size(); i++) {
//                    System.out.println(fileData.get(i));
//                }
                
            } catch (IOException ex) {
                Logger.getLogger(DatabaseTest.class.getName()).log(Level.SEVERE, null, ex);
            }
            
        } catch (FileNotFoundException ex) {
            Logger.getLogger(DatabaseTest.class.getName()).log(Level.SEVERE, null, ex);
        }
        

        conn = null;
        rsmd = null;
        
        

        initialization();
        normalStatement(conn);   
        
        
        
        while(true){
        
            System.out.println("1) Search for lat/long coordinates");
            System.out.println("2) Search for nearby cities OR lat/long coordinates");
            System.out.println("3) Search for nearby cities OR lat/long coordinates");
            
            System.out.println("0) Exit");
            
            
            
        }
//preparedStatement(conn);        
        //callableStatement(conn);
    }

    public static void initialization() {

        try {
            // The newInstance() call is a work around for some
            // broken Java implementations

            Class.forName("com.mysql.jdbc.Driver").newInstance();
        } catch (Exception ex) {
            // handle the error
            System.out.println(ex.toString());
        }

        try {
            conn = DriverManager.getConnection("jdbc:mysql://localhost/GEOLITECITY?" + "user=root&password=moaaz@dell");
            // Do something with the Connection
        } catch (SQLException ex) {
            Logger.getLogger(DatabaseTest.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    public static void normalStatement(Connection conn) {

        try {

            Statement statement = conn.createStatement();
            
            for (int i = 2; i < fileData.size(); i++) {
                
                List<String> tokens = Arrays.asList(fileData.get(i).split("," , -1));
                
                System.out.println(tokens.size());
                
                for (int j = 0; j < tokens.size(); j++) {
                    if (tokens.get(j).length() == 0){
                    
                        //tokens.add(j, "NULL");
                        tokens.set(j, "NULL");
                    }
                }
                
                //System.out.println(tokens.get(8));
                
                int locId = Integer.parseInt(tokens.get(0));
                //String locId = tokens.get(0);
                String country = tokens.get(1);
                String region= tokens.get(2);
                String city = tokens.get(3);
                //int postalCode = Integer.parseInt(tokens.get(4));
                String postalCode = tokens.get(4);
                String latitude = tokens.get(5);
                String longitude = tokens.get(6);
                //double latitude = Double.parseDouble(tokens.get(5));
                //double longitude = Double.parseDouble(tokens.get(6));
                //int metroCode = Integer.parseInt(tokens.get(7));
                //int areaCode = Integer.parseInt(tokens.get(8));
                
                String metroCode = tokens.get(7);
                String areaCode = tokens.get(8);
                
//                String sqlQuery = "INSERT INTO city_inf VALUES(" + locId + ",\"" + country + "\",\"" + region + "\",\"" + city + "\",\"" + postalCode + "\",\"" + latitude + "\",\"" + longitude + "\",\"" + metroCode + "\",\"" + areaCode + "\")" ;
//                statement.execute(sqlQuery);
                
                //System.out.println(locId.toString(), country, region, city, postalCode.toString(), latitude.toString(), longitude.toString(), metroCode.toString(), areaCode.toString());
                
                System.out.print(locId + ",");
                System.out.print(country + ",");
                System.out.print(region + ",");
                System.out.print(city + ",");
                System.out.print(postalCode+ ",");
                System.out.print(latitude+ ",");
                System.out.print(longitude+ ",");
                System.out.print(metroCode+ ",");
                System.out.print(areaCode);
                System.out.println("");
            }
            
//            String sqlQuery = "INSERT INTO city_inf VALUES(" + locId + "," + country ;
//            ResultSet rs = statement.executeQuery(sqlQuery);
//
//            rsmd = rs.getMetaData(); // getColumnCount(), getColumnName(1) , rsmd.getColumnTypeName(1)  
//
//            printDatabaseMetaData(conn);
//
//            int columnCount = rsmd.getColumnCount();
//            System.out.println("# of columns: " + columnCount);
//            System.out.print("Column names: ");
//            for (int i = 1; i <= columnCount; i++) {
//
//                System.out.print("|" + rsmd.getColumnName(i) + "| ");
//
//            }
//
//            System.out.println("");
//
//            // int, String, int, String, int, double, double, int, int
//            while (rs.next()) {
//
//                int id = rs.getInt(1);
//                String firstName = rs.getString(2);
//                String lastName = rs.getString(3);
//                //Date date = rs.getDate(4); // this truncates timestamp to date only.
//                Timestamp ts = rs.getTimestamp(4);
//
//                System.out.println("ID: " + id + " FN: " + firstName + " LN: " + lastName + " TImestamp: " + ts);
//            }

        } catch (SQLException ex) {
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        }

    }

    public static void preparedStatement(Connection conn) {

        try {
            String sqlModificationQuery = "Insert into actor(actor_id, first_name, last_name, last_update) values(?,?,?,?);";

            PreparedStatement prep_statement = conn.prepareStatement(sqlModificationQuery);

            prep_statement.setInt(1, 201);
            prep_statement.setString(2, "Yousaf");
            prep_statement.setString(3, "Ahmad");

            Calendar calendar = Calendar.getInstance();
            java.util.Date now = calendar.getTime();
            java.sql.Timestamp currentTimestamp = new java.sql.Timestamp(now.getTime());

            prep_statement.setTimestamp(4, currentTimestamp);

            int affectedRows = prep_statement.executeUpdate();
            System.out.println("Executed prepared statement. Affected rows: " + affectedRows);

        } catch (SQLException ex) {
            Logger.getLogger(DatabaseTest.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    public static void callableStatement(Connection conn) {

        try {
            CallableStatement callableStatement = conn.prepareCall("{call simpleproc()}",
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);

            String param1 = null;
            //callableStatement.setString(2, param1);
            //callableStatement.setInt(1, 123);

            ResultSet rs = callableStatement.executeQuery(); // if returns ResultSet
            //callableStatement.executeUpdate(); // if updates db... doesn't return ResultSet   

            int count = 0;
            while (rs.next()) {

                count++;

                int id = rs.getInt(1);
                String firstName = rs.getString(2);
                String lastName = rs.getString(3);
                //Date date = rs.getDate(4); // this truncates timestamp to date only.
                Timestamp ts = rs.getTimestamp(4);

                System.out.println("ID: " + id + " FN: " + firstName + " LN: " + lastName + " TImestamp: " + ts);

            }

            System.out.println("ResultSet had rows: " + count);

//            callableStatement.registerOutParameter(1, java.sql.Types.VARCHAR);
//            callableStatement.registerOutParameter(2, java.sql.Types.INTEGER);
        } catch (SQLException ex) {
            Logger.getLogger(DatabaseTest.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    public static void printDatabaseMetaData(Connection conn) {

        try {

            DatabaseMetaData dbmd = conn.getMetaData();

            System.out.println("Driver Name: " + dbmd.getDriverName());
            System.out.println("Driver Version: " + dbmd.getDriverVersion());
            System.out.println("UserName: " + dbmd.getUserName());
            System.out.println("Database Product Name: " + dbmd.getDatabaseProductName());
            System.out.println("Database Product Version: " + dbmd.getDatabaseProductVersion());
        } catch (SQLException ex) {
            Logger.getLogger(DatabaseTest.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
