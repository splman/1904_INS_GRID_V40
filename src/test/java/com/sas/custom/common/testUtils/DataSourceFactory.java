package com.sas.custom.common.testUtils;

import java.sql.*;
import oracle.jdbc.driver.*;
import oracle.jdbc.pool.*;
import com.sas.custom.common.testUtils.mock.database.MockSqlConnection;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

public class DataSourceFactory {
    public enum DataSourceType
    {
        Mock, SQLServer
    }

    public static Map<String, DataSource> getMapJDBC(DataSourceType dataSourceType, boolean autoCommit, String database) throws SQLException {
        switch (dataSourceType)
        {
            case Mock:
                return getMapMockJDBC(autoCommit);
            case SQLServer:
                return getMapSQLServerJDBC(autoCommit, database);
            default:
                throw new UnsupportedOperationException();
        }
    }

    private static Map<String, DataSource> getMapSQLServerJDBC(boolean autoCommit, String database) throws SQLException {
  /*
        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setIntegratedSecurity(false);
        ds.setUser("DCMSSQLAPPL");
        ds.setPassword("$udru3adaqeNE$&b");
        ds.setServerName("SEPLCLDODB01");
        ds.setPortNumber(1433);
        ds.setDatabaseName(database);
        Connection connection = ds.getConnection();
        connection.setAutoCommit(autoCommit);
        return DBUtils.createMapWithIOActivity(connection);
*/
//        OracleDataSource ds = new OracleDataSource();
//        ds.setDriverType("thin");
//        ds.setServerName("localhost");
//        ds.setPortNumber(1521);
//        ds.setDatabaseName("30");
//        ds.setUser("HR");
//        ds.setPassword("Swanna123");
//
//        Connection connection = ds.getConnection();

        String url = "jdbc:oracle:thin:@//localhost:1521/morcl1";
        String username = "HR";
        String password = "Swanna123";

        Connection connection = DriverManager.getConnection(url, username, password);

        connection.setAutoCommit(autoCommit);
        return DBUtils.createMapWithIOActivity(connection);

    }

    private static Map<String, DataSource> getMapMockJDBC(boolean autoCommit) throws SQLException {
        return DBUtils.createMapWithIOActivity(new MockSqlConnection());
    }
}
