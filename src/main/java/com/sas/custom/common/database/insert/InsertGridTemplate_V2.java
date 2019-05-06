package com.sas.custom.common.database.insert;

import com.sas.analytics.ph.common.RTDMTable;

import com.sas.analytics.ph.common.jaxb.DataTypes;
import java.sql.*;
import java.util.*;
import org.apache.log4j.Logger;

// bez import com.sas.custom.common.database.DatabaseTemplate;
import com.sas.custom.common.database.DatabaseTemplate_V2;

//import com.sas.custom.common.database.DatabaseUtils;
//import com.sas.custom.common.database.PreparedStatementCreator;

//import com.sas.custom.common.database.decorators.ConnectionDecorator;
//import com.sas.custom.common.grid.AppendColumn;
//import com.sas.custom.common.grid.RowFilter;




//public class InsertGridTemplate  {
public class InsertGridTemplate_V2 extends DatabaseTemplate_V2 {
    private static final Logger logger = Logger.getLogger(InsertGridTemplate_V2.class);

    private static final String INSERT_SEPARATOR = ", ";
    private static final String INSERT_SEPARATOR_MULTI = " ";

    private String tableName;
    //private List<RowFilter> rowFilters;

    /*
        if columnMap is null - do not use any columns from grid
        if columnMap is empty - use all columns from grid
     */
    private Map<String, String> columnMap;
    //private List<AppendColumn> appendColumns;

    public InsertGridTemplate_V2()
    {
        this(null, null);
    }

    //public InsertGridTemplate_V2(String tableName,  Map<String, String> columnMap, List<AppendColumn> appendColumns)
    public InsertGridTemplate_V2(String tableName,  Map<String, String> columnMap )
    {
        this.tableName = tableName;
        //this.appendColumns = appendColumns == null ? new LinkedList<AppendColumn>() : appendColumns;
        this.columnMap = columnMap;

    }
//    public InsertGridTemplate_V2(String tableName, List<RowFilter> rowFilters, Map<String, String> columnMap, List<AppendColumn> appendColumns)
//    {
//        this.tableName = tableName;
//        this.appendColumns = appendColumns == null ? new LinkedList<AppendColumn>() : appendColumns;
//        this.columnMap = columnMap;
//        this.rowFilters = rowFilters == null ? new LinkedList<RowFilter>() : rowFilters;
//    }

//    public int insertAll(PreparedStatementCreator preparedStatementCreator, RTDMTable inputGrid, String tableName) throws SQLException {
//        this.tableName = tableName;
//        return insertAll(preparedStatementCreator, inputGrid, 0);
//    }

//RTDMTableUtils
    public static List<RTDMTable.Row> filterRows(RTDMTable inputGrid)
    {
        List<RTDMTable.Row> rowsToSave = new LinkedList<>();
        Iterator<RTDMTable.Row> iterator = inputGrid.iterator();
        while(iterator.hasNext()) {
            RTDMTable.Row row = iterator.next();
            rowsToSave.add(row);
        }
        return rowsToSave;
    }
    //DatabaseUtils
    public static int getSQLType(DataTypes type) {
        switch (type)
        {
            case STRING:
                return Types.VARCHAR;
            case INT:
                return Types.INTEGER;
            case FLOAT:
                return Types.FLOAT;
            case DATETIME:
                return Types.TIMESTAMP;
            case BOOLEAN:
                return Types.BOOLEAN;
            case TABLE:
            case ANY:
            default:
                throw new UnsupportedOperationException();
        }
    }

    public static void bindVariable(PreparedStatement preparedStatement, int idx, DataTypes type, Object value) throws SQLException {
        if(value == null)
        {
            //preparedStatement.setNull(idx, DatabaseUtils.getSQLType(type));
            preparedStatement.setNull(idx, getSQLType(type));
            return;
        }
        switch (type)
        {
            case STRING:
                preparedStatement.setString(idx, (String) value);
                break;
            case INT:
                preparedStatement.setLong(idx, (Long) value);
                break;
            case FLOAT:
                preparedStatement.setDouble(idx, (Double) value);
                break;
            case DATETIME:
                preparedStatement.setTimestamp(idx, new Timestamp(((Calendar) value).getTimeInMillis()));
                break;
            case BOOLEAN:
                preparedStatement.setBoolean(idx, (Boolean) value);
                break;
            case TABLE:
            case ANY:
                throw new UnsupportedOperationException();
        }
    }



    public int insertAll( RTDMTable inputGrid, int rowNumber) throws SQLException {
/*       lista wierszy z grida */
        logger.trace("inputGrid : "+inputGrid.toString() );
        //List<RTDMTable.Row> rowsToSave = inputGrid == null ? new LinkedList<RTDMTable.Row>() : RTDMTableUtils.filterRows(inputGrid, rowFilters);
        List<RTDMTable.Row> rowsToSave = inputGrid == null ? new LinkedList<RTDMTable.Row>() : filterRows(inputGrid);

        int rowsCnt = rowsToSave.isEmpty() ? rowNumber : rowsToSave.size();
        if(rowsCnt < 1) {
            logger.warn("No rows in inputGrid and rowNumber is not positive while inserting grid to table: " + tableName);
            return 0;
        }
        Map<RTDMTable.Column, String> columnToInsertMap = new LinkedHashMap<>();
        if(columnMap != null)
            for(RTDMTable.Column c : inputGrid.getColumns()) {
                String fieldName = columnMap.isEmpty() ? c.getName() : columnMap.get(c.getName());
                if(fieldName != null)
                    columnToInsertMap.put(c, fieldName);
            }
        else if (inputGrid != null){
            for(RTDMTable.Column c : inputGrid.getColumns()) {
                columnToInsertMap.put(c, c.getName());

            }
        }
        //if(columnToInsertMap.isEmpty() && appendColumns.isEmpty())
        if(columnToInsertMap.isEmpty() )
            throw new RuntimeException("No column specified");

        logger.trace("preparedStatementCreator: " );
        PreparedStatement ps = this.createStatement(createStatement(columnToInsertMap, rowsCnt));
        int variableIterator = 0;
        int rowNum = 0;
        for(int i=0; i<rowsCnt; i++)
        {
            RTDMTable.Row row = rowsToSave.isEmpty() ? null : rowsToSave.get(i);
            if(row != null)
                for(RTDMTable.Column c : columnToInsertMap.keySet())
                    //DatabaseUtils.bindVariable(ps, ++variableIterator, c.getType(), row.columnDataGet(c.getName()));
                    bindVariable(ps, ++variableIterator, c.getType(), row.columnDataGet(c.getName()));
            //for(AppendColumn appendColumn : appendColumns)
                //DatabaseUtils.bindVariable(ps, ++variableIterator, appendColumn.getType(), appendColumn.getValueCreator().createValue(row, rowNum));
                //bindVariable(ps, ++variableIterator, appendColumn.getType(), appendColumn.getValueCreator().createValue(row, rowNum));
            ++rowNum;
        }
        return ps.executeUpdate();
    }

//    private String bkp_createStatement(Map<RTDMTable.Column, String> colmnsToInsertMap, int rowCnt) {
//        StringBuffer sb = new StringBuffer();
//        sb.append("INSERT INTO ").append(tableName).append(" (");
//        createColumnList(sb, colmnsToInsertMap);
//        sb.append(") VALUES ");
//        String rowBindedListStr = createBindedList(colmnsToInsertMap);
//        for(int i=0; i<rowCnt; i++)
//            sb.append(rowBindedListStr).append(INSERT_SEPARATOR);
//        sb.setLength(sb.length() - INSERT_SEPARATOR.length());
//        return sb.toString();
//    }

    private String createStatement(Map<RTDMTable.Column, String> colmnsToInsertMap, int rowCnt) {
        StringBuffer sb = new StringBuffer();
        sb.append("INSERT ALL  ");
        for(int i=0; i<rowCnt; i++) {
            sb.append(" INTO ").append(tableName).append(" (");
            createColumnList(sb, colmnsToInsertMap);
            sb.append(") VALUES ");
            String rowBindedListStr = createBindedList(colmnsToInsertMap);
            sb.append(rowBindedListStr).append(INSERT_SEPARATOR_MULTI);
        }
        sb.setLength(sb.length() - INSERT_SEPARATOR_MULTI.length());
        sb.append(" SELECT * FROM dual ");
        return sb.toString();
    }

    private String createBindedList(Map<RTDMTable.Column, String> colmnsToInsert) {
        StringBuilder sb = new StringBuilder();
        sb.append('(');
        for(RTDMTable.Column c : colmnsToInsert.keySet())
            sb.append('?').append(INSERT_SEPARATOR);
//        for(AppendColumn appendColumn : appendColumns)
//            sb.append('?').append(INSERT_SEPARATOR);
        sb.setLength(sb.length() - INSERT_SEPARATOR.length());
        sb.append(')');
        return sb.toString();
    }

    private void createColumnList(StringBuffer sb, Map<RTDMTable.Column, String> colmnsToInsert) {
        for(RTDMTable.Column c : colmnsToInsert.keySet())
            sb.append(colmnsToInsert.get(c)).append(INSERT_SEPARATOR);
//        for(AppendColumn appendColumn : appendColumns)
//            sb.append(appendColumn.getName()).append(INSERT_SEPARATOR);
        sb.setLength(sb.length() - INSERT_SEPARATOR.length());
    }
}
