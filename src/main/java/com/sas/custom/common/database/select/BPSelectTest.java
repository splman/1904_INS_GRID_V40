package com.sas.custom.common.database.select;



import com.sas.analytics.ph.common.exp.SymbolTable;
import com.sas.analytics.ph.common.jaxb.DataTypes;
import org.apache.log4j.Logger;

import oracle.jdbc.*;
import oracle.sql.ArrayDescriptor;
import oracle.sql.ARRAY;
import oracle.jdbc.OracleTypes;
import java.sql.*;
import java.util.*;

import javax.sql.DataSource;
import javax.naming.Context;
import javax.naming.InitialContext;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.math.BigDecimal;
import java.util.ArrayList;

class BPSelectTest implements Runnable {

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
                        if ( rs != null ) {
                            while (rs.next())
                            {
                                String offerCd = rs.getString("OFFER_TYPE_CD");
                                Integer rulCd = rs.getInt("VERIFICATION_VAR");
                                log.debug( "OUT resultList (offerCd,rulCd) : " +offerCd+" "+ rulCd.toString());
                                //resultList.put(offerCd,rulCd);
                            }
                            rs.close();
                        }
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
}