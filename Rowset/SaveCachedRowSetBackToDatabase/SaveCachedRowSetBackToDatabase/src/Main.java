import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.Types;

import javax.sql.RowSetMetaData;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetMetaDataImpl;

import com.sun.rowset.CachedRowSetImpl;

public class Main {
  public static void main(String[] args) throws Exception {
    Connection conn = getHSQLConnection();
    System.out.println("Got Connection.");
    Statement st = conn.createStatement();
    st.executeUpdate("create table survey (id int,name varchar);");
    st.executeUpdate("create view surveyView as (select * from survey);");
    st.executeUpdate("insert into survey (id,name ) values (1,'nameValue')");
    st.executeUpdate("insert into survey (id,name ) values (2,'anotherValue')");    
    
    CachedRowSet crs = null;
    RowSetMetaData rsMD = new RowSetMetaDataImpl();
    rsMD.setColumnCount(2);
    rsMD.setColumnName(1, "id");
    rsMD.setColumnType(1, Types.VARCHAR);
    rsMD.setColumnName(2, "name");
    rsMD.setColumnType(2, Types.VARCHAR);
    // sets the designated column's table name, if any, to the given String.
    rsMD.setTableName(1, "survey");
    rsMD.setTableName(2, "survey");

    // use a custom made RowSetMetaData object for CachedRowSet object
    crs = new CachedRowSetImpl();
    crs.setMetaData(rsMD);

    crs.moveToInsertRow();
    crs.updateString(1, "1111");
    crs.updateString(2, "alex");
    crs.insertRow();

    crs.moveToInsertRow();
    crs.updateString(1, "2222");
    crs.updateString(2, "jane");
    crs.insertRow();

    // if you want to commit changes from a CachedRowSet
    // object to your desired datasource, then you must
    // create a Connection object.
    //
    //conn = getHSQLConnection();

    // moves the cursor to the remembered cursor position, usually
    // the current row. This method has no effect if the cursor is
    // not on the insert row.
    crs.moveToCurrentRow();

    // when the method acceptChanges() is executed, the CachedRowSet
    // object's writer, a RowSetWriterImpl object, is called behind the
    // scenes to write the changes made to the rowset to the underlying
    // data source. The writer is implemented to make a connection to
    // the data source and write updates to it.
    crs.acceptChanges(conn);
    conn.close();
  }

  private static Connection getHSQLConnection() throws Exception {
    Class.forName("org.hsqldb.jdbcDriver");
    System.out.println("Driver Loaded.");
    String url = "jdbc:hsqldb:data/tutorial";
    return DriverManager.getConnection(url, "sa", "");
  }

  public static Connection getMySqlConnection() throws Exception {
    String driver = "org.gjt.mm.mysql.Driver";
    String url = "jdbc:mysql://localhost/demo2s";
    String username = "oost";
    String password = "oost";

    Class.forName(driver);
    Connection conn = DriverManager.getConnection(url, username, password);
    return conn;
  }

  public static Connection getOracleConnection() throws Exception {
    String driver = "oracle.jdbc.driver.OracleDriver";
    String url = "jdbc:oracle:thin:@localhost:1521:caspian";
    String username = "mp";
    String password = "mp2";

    Class.forName(driver); // load Oracle driver
    Connection conn = DriverManager.getConnection(url, username, password);
    return conn;
  }
}
