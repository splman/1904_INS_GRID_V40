package com.sas.custom.common.database.select

import com.sas.custom.common.database.select.Select2Dashboard;

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
import org.apache.log4j.Logger

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
import java.sql.*;
import java.sql.PreparedStatement;


import com.sas.custom.common.database.select.BPSelectTest;

class BPSelectTestTest  extends GroovyTestCase  {

    private DataSourceFactory.DataSourceType dataSourceType;
    private boolean autoCommit;
    //private Map<String, DataSource> mapJdbc;

    //private DatabaseTemplate databaseTemplate = new DatabaseTemplate() {}

//============
    //private static final String TABLE_NAME = "S1600399.TEST_TABLE"
    private static final String TABLE_NAME = "HR.TEST_TABLE"

    private static final String APPEND_STRING_COL = "APPEND_STRING_COL"

    void testResultSet2Grid() {
        def tableName = 'DCMS_Test_TMP.DCMS_LR.OFFER_STATUS'
        def columnName = 'CUSTOMER_SK'
        def grid = new RTDMTable()

        grid.columnAdd(columnName, DataTypes.INT, Collections.emptyList())
        RTDMTable.Row row = grid.rowAdd()
        row.columnDataSet(columnName, new Long(2458044))
        row = grid.rowAdd()
        row.columnDataSet(columnName, new Long(2458045))


        //def rs = createStatement("SELECT CUSTOMER_SK FROM $tableName WHERE OFFER_ID IN (1, 2)").executeQuery()
        //def gridFromST = st.resultSetToGrid(rs, rs.getMetaData())

        //assertEquals(gridFromST, grid)
        //cleanUp()

        def log = Logger.getLogger("com.sas.vp.dashboard.grov.Select2Dashboard")
        Map<String, DataSource> mapJdbc2 = DataSourceFactory.getMapJDBC(DataSourceFactory.DataSourceType.SQLServer, false, "DCMS");
        def selDash = new Select2Dashboard(tableName)
        selDash.setMapJDBC(mapJdbc2);


        selDash.run();
        log.info( " Grid_Out: "+selDash.OUT_GRID.toString())
    }





    void testNoColumnsSpecified()
    {


        Map<String, DataSource> mapJdbc = DataSourceFactory.getMapJDBC(DataSourceFactory.DataSourceType.SQLServer, true, "DCMS");
        BPSelectTest selTst = new BPSelectTest();
        selTst.setMapJDBC(mapJdbc);


        selTst.run();
    }



    public Map<String, DataSource> getDataSourceMap()
    {
        return mapJdbc;
    }



    private Connection connection;

    private Connection getConnection()
    {
        if(connection == null)
            connection = DBUtils.getConnection(getDataSourceMap());
        return connection;
    }




}