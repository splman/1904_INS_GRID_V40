package com.sas.custom.common.database.select;

import com.sas.analytics.ph.common.RTDMTable;
import com.sas.analytics.ph.common.jaxb.DataType;
import com.sas.analytics.ph.common.jaxb.DataTypes;
//import com.sas.custom.common.database.PreparedStatementCreator;
import org.codehaus.groovy.runtime.DateGroovyMethods;
import org.codehaus.groovy.runtime.DefaultGroovyMethods;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by splkon.
 */
public class SelectTemplate {
    private String tableName;
    private String columnList = "*";

    public SelectTemplate(){}

    public SelectTemplate(String tableName) {
        setTableName(tableName);
    }

    public SelectTemplate(String tableName, String columnList) {
        setTableName(tableName);
        this.columnList = columnList;
    }

    public RTDMTable convertStatementToGrid(PreparedStatement preparedStatement) {
        try {
            ResultSet rs = preparedStatement.executeQuery();
            return resultSetToGrid(rs, rs.getMetaData());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private DataTypes convertType(ResultSetMetaData rsmd, int idx) throws SQLException {
        String columnTypeName = rsmd.getColumnTypeName(idx).toLowerCase();
        switch(columnTypeName)
        {
            case "numeric":
                return (rsmd.getScale(idx) == 0) ? DataTypes.INT : DataTypes.FLOAT;
            case "bit":
                return DataTypes.BOOLEAN;
            case "nchar":
            case "char":
            case "nvarchar":
            case "varchar":
            case "uniqueidentifier":
                return DataTypes.STRING;
            case "tinyint":
            case "int":
            case "smallint":
            case "bigint":
                return DataTypes.INT;
            case "decimal":
            case "money":
            case "smallmoney":
            case "float":
//            case "numeric":
                return DataTypes.FLOAT;
            case "datetime":
            case "datetime2":
            case "date":
                return DataTypes.DATETIME;
            default:
                throw new UnsupportedOperationException("Unsupported conversion from type=" + columnTypeName);
        }
    }

    public RTDMTable resultSetToGrid(final ResultSet rs, final ResultSetMetaData rsmd) {
        final RTDMTable grid = new RTDMTable();
        try {
            for(int i=0; i<rsmd.getColumnCount(); i++) {
                String colName = rsmd.getColumnName(i + 1);
                DataTypes colType = convertType(rsmd, i + 1);
                grid.columnAdd(colName, colType, new LinkedList<Object>());
            }
            for (int c = 0; rs.next(); c++) {
                final RTDMTable.Row row = grid.rowAdd();
                for(int i=0; i<rsmd.getColumnCount(); i++) {
                    if (rs.getObject(i+1)!= null) {
                        switch (grid.columnGet(i).getType()) {
                            case STRING:
                                row.columnDataSet(i, rs.getString(i + 1), false);
                                break;
                            case INT:
                                row.columnDataSet(i, rs.getLong(i + 1), false);
                                break;
                            case BOOLEAN:
                                row.columnDataSet(i, rs.getBoolean(i + 1), false);
                                break;
                            case FLOAT:
                                row.columnDataSet(i, rs.getDouble(i + 1), false);
                                break;
                            case DATETIME:
                                Timestamp timestamp = rs.getTimestamp(i + 1);
                                row.columnDataSet(i, timestamp == null ? null : DateGroovyMethods.toCalendar(timestamp), false);
                                break;
                        }
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return grid;
    }

//    public ResultSet select(PreparedStatementCreator psc, String whereField, List<Long> whereValues) throws SQLException {
//        if (whereValues == null || whereValues.isEmpty())
//            throw new RuntimeException("Value list is empty.");
//        whereValues = getDistinctList(whereValues);
//        PreparedStatement ps = psc.createStatement(buildQuery(whereField, whereValues));
//        bindVariables(ps, whereValues);
//        return ps.executeQuery();
//    }

    private List<Long> getDistinctList(List<Long> list){
        List<Long> result = new LinkedList<>();
        for(Long l : list)
            if(!result.contains(l))
                result.add(l);
        return result;
    }

    private String buildQuery(String whereField, List whereValues) {
        String sql = "SELECT " + columnList + " FROM " + tableName + " WHERE " + whereField + " IN (?";
        for (int i = 1; i < whereValues.size(); i++)
            sql += ", ?";
        sql += ")";
        return sql;
    }

    private void bindVariables(final PreparedStatement ps, final List<Long> whereValues) {
        try {
            for (int i = 0; i < whereValues.size(); i++)
                ps.setLong(i + 1, whereValues.get(i));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public static <T> StringBuilder generateInParamsForQuery(List<T> list) {
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        if(list.isEmpty()){
            sb.append("null");
        }else{
            for (int i = 0; i < list.size(); i++) {
                sb.append('?');
                if (list.size() > i + 1) {
                    sb.append(", ");
                }
            }
        }
        sb.append(")");
        return sb;
    }
}
