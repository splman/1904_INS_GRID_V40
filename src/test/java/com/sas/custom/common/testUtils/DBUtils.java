package com.sas.custom.common.testUtils;

//import com.sas.custom.common.database.DatabaseTemplate;
import com.sas.custom.common.testUtils.mock.database.MockSqlConnection;
import com.sas.custom.common.testUtils.mock.database.MockSqlDataSource;
import org.junit.Assert;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DBUtils {
    private static final String GENERAL_IO_ACTIVITY_RESOURCE= "GeneralIO_Activity_Resource";

    public static Map<String, DataSource> createMapWithIOActivity(final Connection connection)
    {
        MockSqlDataSource dataSource = new MockSqlDataSource();
        dataSource.setConnectionProvider(new MockSqlDataSource.ConnectionProvider() {
            @Override
            public Connection getConnection() {
                return connection;
            }
        });
        Map<String, DataSource> result = new LinkedHashMap<>();
        result.put(GENERAL_IO_ACTIVITY_RESOURCE, dataSource);
        return result;
    }

    public static Connection getConnection(final Map<String, DataSource> map) throws SQLException {
        return map.get(GENERAL_IO_ACTIVITY_RESOURCE).getConnection();
    }

//    public static int getExecutedStatementCount(DatabaseTemplate databaseTemplate){
//        if(databaseTemplate.getDataSource() instanceof MockSqlDataSource) {
//            try {
//                Connection conn = databaseTemplate.getDataSource().getConnection();
//                return ((MockSqlConnection) conn).getCreatedStatements().size();
//            } catch (SQLException e) {
//                throw new RuntimeException(e);
//            }
//        }
//        return 0;
//    }

    public static String getExecutedStatement(DataSource dataSource, int idx){
        if(dataSource instanceof MockSqlDataSource) {
            return ((MockSqlConnection) ((MockSqlDataSource) dataSource).getLastCreatedConnection()).getCreatedStatements().get(idx).getStatement();
        }
        return null;
    }

//    public static void assertStatement(String statement, DatabaseTemplate databaseTemplate, int idx) {
//        if(databaseTemplate.getDataSource() instanceof MockSqlDataSource) {
//            System.out.println("Testing statement: " + statement);
//            Assert.assertEquals(statement, DBUtils.getExecutedStatement(databaseTemplate.getDataSource(), idx));
//        }
//    }

//    public static PreparedStatementCreator getPreparedStatementCreator(final Connection connection) throws SQLException {
//        return new PreparedStatementCreator() {
//            @Override
//            public PreparedStatement createStatement(String statementStr) {
//                try {
//                    return connection.prepareStatement(statementStr);
//                } catch (SQLException e) {
//                    throw new RuntimeException(e);
//                }
//            }
//        };
//    }
}
