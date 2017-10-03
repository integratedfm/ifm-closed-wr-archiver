package ifm.db;

import ifm_cqu.IfmCQU;
import static ifm_cqu.IfmCQU.AddError;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.log4j.Logger;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author ifmuser1
 */
public class MSSqlJDBC {

    final static Logger logger = Logger.getLogger(MSSqlJDBC.class);
    private static final String DB_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    private static String DB_CONNECTION = null;
    // "jdbc:sqlserver://127.0.0.1:1433;databaseName=mssql-hq-2014-0918;"
    //+ "user=afm;password=afm";
    private static String DB_USER;
    private static String DB_PASSWORD;
    private static Connection dbConnection = null;

    public static Connection getSqlServerDBConnection() {

        if (dbConnection == null) {
            
            DB_CONNECTION = IfmCQU.getProp("ifm.cqu.dbconnection");
            DB_USER = IfmCQU.getProp("ifm.cqu.db_user");
            DB_PASSWORD = IfmCQU.getProp("ifm.cqu.db_passwd");

            try {
                Class.forName(DB_DRIVER);

            } catch (ClassNotFoundException e) {
                logger.error(e.getMessage());

            }
            try {
                dbConnection = DriverManager.getConnection(
                        DB_CONNECTION);
                dbConnection.setAutoCommit(false);
                return dbConnection;

            } catch (SQLException e) {
                logger.error(e.getMessage());
                AddError(e.getMessage());
            }
        }

        return dbConnection;
    }

   
    public static PreparedStatement getPreparedStatement(String sql) {
        PreparedStatement pstmt = null;
        try {
            Connection con = getSqlServerDBConnection();
            pstmt = con.prepareStatement(sql);
            return pstmt;

        } catch (SQLException ex) {
            //log error
            AddError(ex.getMessage());
        }
        return pstmt;
    }

}
