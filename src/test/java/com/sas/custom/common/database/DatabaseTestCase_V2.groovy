package com.sas.custom.common.database

import com.sas.analytics.ph.common.RTDMTable
import com.sas.analytics.ph.common.jaxb.DataTypes
//import com.sas.custom.common.database.decorators.CallableStatementDecorator
//import com.sas.custom.common.database.decorators.PreparedStatementDecorator
import com.sas.custom.common.database.insert.InsertGridTemplate_V2
import com.sas.custom.common.grid.AppendColumn
//import com.sas.custom.common.grid.GridColumnDefinition
import com.sas.custom.common.grid.RowFilter
import com.sas.custom.common.grid.ValueCreator

import com.sas.custom.common.testUtils.DBUtils
import com.sas.custom.common.testUtils.DataSourceFactory
//import com.sas.custom.common.testUtils.mock.database.MockSqlConnection

import javax.sql.DataSource
import java.sql.CallableStatement
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.SQLException


import com.sas.analytics.ph.common.jaxb.DataTypes;
import com.sas.custom.common.grid.GridColumnDefinition;
//================
import com.sas.analytics.ph.common.RTDMTable;
import com.sas.analytics.ph.common.jaxb.DataTypes;
import com.sas.custom.common.grid.GridColumnDefinition;

import java.sql.Timestamp;
import java.util.*;
import com.sas.custom.common.grid.GridColumnDefinition;



 class DatabaseTestCase_V2 extends GroovyTestCase  {

    private DataSourceFactory.DataSourceType dataSourceType;
    private boolean autoCommit;
    //private Map<String, DataSource> mapJdbc;

    //private DatabaseTemplate databaseTemplate = new DatabaseTemplate() {}
     private DatabaseTemplate_V2 databaseTemplate ;
//============
     private static final String TABLE_NAME = "HR.TEST_TABLE"

     private static final String APPEND_STRING_COL = "APPEND_STRING_COL"

     private RTDMTable testRTDMTable;
     private List<RowFilter> oneRowFilterList;
     private LinkedHashMap<String, String> columnMap
     private ArrayList<AppendColumn> appendColumnList

     public static final String STRING_COL_NAME = "TEST_STRING_COL";
        public static final String LONG_COL_NAME = "TEST_LONG_COL";

     public static final String DOUBLE_COL_NAME = "TEST_DOUBLE_COL";
     public static final String DATE_COL_NAME = "TEST_DATE_COL";
     public static final String BOOLEAN_COL_NAME = "TEST_BOOLEAN_COL";



     //public static final Long[] LONG_VALUES = {4144141234L, 0L, null, -5L};

     public static final List<GridColumnDefinition> TEST_COLUMNS2 = Arrays.asList(
             new GridColumnDefinition(STRING_COL_NAME, DataTypes.STRING),
             new GridColumnDefinition(LONG_COL_NAME, DataTypes.INT),
//             new GridColumnDefinition(DOUBLE_COL_NAME, DataTypes.FLOAT),
//             new GridColumnDefinition(DATE_COL_NAME, DataTypes.DATETIME),
//             new GridColumnDefinition(BOOLEAN_COL_NAME, DataTypes.BOOLEAN)
     );

     void testNoColumnsSpecified()
     {

         //databaseTemplate = new DatabaseTemplate() {};
         //setParams(DataSourceFactory.DataSourceType.Mock, false)
         appendColumnList = []
         appendColumnList.add(new AppendColumn("APPEND_STRING_COL", DataTypes.STRING, new ValueCreator() {
             @Override
             Object createValue(RTDMTable.Row row, int rowNum) {
                 return "TEST_VALUE";
             }
         }))
         def LONG_VALUES = [1l, 2l, 3l, 4l]
         def STRING_VALUES = ['1l', '2l', '3l', '4l']
         //RTDMTable test2RTDMTable = RTDMTableTestUtils.createTestRTDMTable(TEST_COLUMNS2);
         List<GridColumnDefinition> columns=TEST_COLUMNS2;
         RTDMTable test2RTDMTable = new RTDMTable();


         def LONG_COL_LST = [111l, 222l, 333l, 444l]


         test2RTDMTable.columnAdd(LONG_COL_NAME, (Long[]) LONG_COL_LST.toArray())
         Map<String, DataSource> mapJdbc = DataSourceFactory.getMapJDBC(DataSourceFactory.DataSourceType.SQLServer, true, "DCMS");
         DataSource dataSource = mapJdbc.get("GeneralIO_Activity_Resource");


        // println toString(test2RTDMTable)
         InsertGridTemplate_V2 insertGridTemplate = new InsertGridTemplate_V2(TABLE_NAME, null)

         //insertGridTemplate.setMapJDBC(mapJdbc);
         Connection  dbCon =  insertGridTemplate.getConnection(dataSource);

         //insertGridTemplate.setDataSource(ds)
         int rc = insertGridTemplate.insertAll( test2RTDMTable, 0)
         //Thread.sleep(3000);
         insertGridTemplate.cleanUp();
         //assertEquals(1, DBUtils.getExecutedStatementCount(databaseTemplate))
//         DataSource ds=insertGridTemplate.getDataSource()
//         String exSQL=DBUtils.getExecutedStatement(ds, 0)
//         assertEquals('INSERT INTO HR.TEST_TABLE (TEST_STRING_COL, TEST_LONG_COL, TEST_DOUBLE_COL, TEST_DATE_COL, TEST_BOOLEAN_COL) VALUES (\'ABC\', 4144141234, 4.1234132322E8, null, 0), (\'\', 0, null, \'2001-09-09 03:46:40.0\', null), (\'  ĄŹ  \', null, -2.123123123, null, 1), (null, -5, 0.0, \'2001-09-09 03:46:40.0\', null)',exSQL )
//
        // cleanUp(dbCon)
         //cleanUp()

         //Map<String, DataSource> mapJdbc = DataSourceFactory.getMapJDBC(DataSourceFactory.DataSourceType.SQLServer, true, "DCMS");
         //BPSelectTest selTst = new BPSelectTest();

         Map<String, DataSource> mapJdbc2 = DataSourceFactory.getMapJDBC(DataSourceFactory.DataSourceType.SQLServer, false, "DCMS");
         InsertGridTemplate_V2 insertGridTemplate2 = new InsertGridTemplate_V2(TABLE_NAME, null)
         insertGridTemplate2.setMapJDBC(mapJdbc2);

         insertGridTemplate2.run();

     }



    public Map<String, DataSource> getDataSourceMap()
    {
        return mapJdbc;
    }

//    public PreparedStatement createStatement(String statementStr) {
//        try {
//            return new PreparedStatementDecorator(getConnection().prepareStatement(statementStr), statementStr);
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//    @Override
//    CallableStatement createCallStatement(String statementStr) {
//        try {
//            return new CallableStatementDecorator(getConnection().prepareCall(statementStr), statementStr);
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
//    }

    private Connection connection;

    private Connection getConnection()
    {
        if(connection == null)
            connection = DBUtils.getConnection(getDataSourceMap());
        return connection;
    }

//     public void cleanUp(Connection pconnection)
//     {
//         pconnection.close();
//         if(pconnection instanceof MockSqlConnection)
//             pconnection.createdStatements.clear()
//     }
//
//
//
//    public void cleanUp()
//    {
//        connection?.close()
//        if(connection instanceof MockSqlConnection)
//            connection.createdStatements.clear()
//    }

    public DatabaseTemplate_V2 getDatabaseTemplate_V2()
    {
        return databaseTemplate_V2;
    }

//    public void testAfterExecution(DatabaseTemplate databaseTemplate, Integer expectedReturnCode, Integer actualReturnCode, String sqlInstruction)
//    {
//        def connection = databaseTemplate.connection
//        if(connection instanceof  MockSqlConnection)
//        {
//            def statement = connection.lastCreatedStatement.statement
//            assertEquals(sqlInstruction, statement)
//        }
//        else
//        {
//            if(expectedReturnCode != null)
//                assertEquals(expectedReturnCode, actualReturnCode)
//        }
//    }

//    public void testAfterExecution(DatabaseTemplate databaseTemplate, List<String> sqlInstructionList)
//    {
//        if(connection instanceof  MockSqlConnection)
//            for(int i=0; i<connection.createdStatements; i++)
//                assertEquals(sqlInstructionList.get(i), connection.createdStatements.get(i))
//    }
}
