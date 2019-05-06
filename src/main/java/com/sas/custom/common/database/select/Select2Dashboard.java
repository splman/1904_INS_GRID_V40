package com.sas.custom.common.database.select;

import com.sas.analytics.ph.common.RTDMTable;
import com.sas.analytics.ph.common.jaxb.DataTypes;
import org.apache.log4j.Logger;
import org.codehaus.groovy.runtime.DateGroovyMethods;

import javax.sql.DataSource;
import java.sql.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by splman on 5/6/2019.
 */




/**
 * Created by splkon.
 */
public class Select2Dashboard implements Runnable  {

    RTDMTable OUT_GRID;

    private String tableName;
    private String columnList = "*";

    private static final Logger log = Logger.getLogger("com.sas.bp.grov.BPSelectTest");
    private Map<String,DataSource> mapJDBC = null;


    public void setMapJDBC(Map<String,DataSource> input){
        if(input == null)
            log.error("MapJDBC is NULL!!");
        mapJDBC = input;
    }



    public void run() throws RuntimeException  {

        Connection connection = null;
        String pFromTable = "HR.TEST_TABLE";
        PreparedStatement psVerifi = null;
        ResultSet rs = null;
        try {

            connection = mapJDBC.get("GeneralIO_Activity_Resource").getConnection();
            if (connection != null) {

                StringBuilder selQuery = new StringBuilder();
                selQuery.append(" select TEST_STRING_COL as OFFER_TYPE_CD, TEST_LONG_COL as VERIFICATION_VAR from ");
                selQuery.append(pFromTable+" ");
                String sqlVerifi=selQuery.toString();
                log.debug( "PROC sqlVerifi : " +sqlVerifi );
                if ( ! sqlVerifi.isEmpty() ) {
                    try {
                        psVerifi = connection.prepareStatement(sqlVerifi);

//                        for (int iq=1;iq <= queryNr ;iq++){
//                            psVerifi.setDouble(iq, pCifKey);
//                        }
                        rs = psVerifi.executeQuery();
                        // rs = createStatement("SELECT CUSTOMER_SK FROM $tableName WHERE OFFER_ID IN (1, 2)").executeQuery();
                        OUT_GRID = resultSetToGrid(rs, rs.getMetaData());
                        log.debug(OUT_GRID.toString());
//                        if ( rs != null ) {
//                            while (rs.next())
//                            {
//                                String offerCd = rs.getString("OFFER_TYPE_CD");
//                                Integer rulCd = rs.getInt("VERIFICATION_VAR");
//                                log.debug( "OUT resultList (offerCd,rulCd) : " +offerCd+" "+ rulCd.toString());
//                                //resultList.put(offerCd,rulCd);
//                            }
//                            rs.close();
//                        }
                    }
                    catch(SQLException e)
                    {
                        log.error( "ERROR Reading " + pFromTable);
                        throw new RuntimeException(e);
                    }
                    finally
                    {   if (psVerifi != null ) {
                        psVerifi.close();
                    }
                    }
                }


            } else {
                //Throwable e;
                log.error("ERROR Nie uzyskano polaczenia dla zasobu Oracle OPERdb connection. ");
                throw new RuntimeException();
            }

        } catch (Exception e) {
            log.error("ERROR Nie wykonano walidacji warunkow dla kampanii. ",e);
            throw new RuntimeException(e);
        }
        finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    public Select2Dashboard(){}

    public Select2Dashboard(String tableName) {
        setTableName(tableName);
    }

    public Select2Dashboard(String tableName, String columnList) {
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
            case "number":
                return (rsmd.getScale(idx) == 0) ? DataTypes.INT : DataTypes.FLOAT;
            case "bit":
                return DataTypes.BOOLEAN;
            case "nchar":
            case "char":
            case "nvarchar":
            case "varchar":
            case "varchar2":
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
