package com.sas.custom.common.database.select



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