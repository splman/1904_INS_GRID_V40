package com.sas.custom.common.database;

//import com.sas.custom.common.database.decorators.CallableStatementDecorator;
//import com.sas.custom.common.database.decorators.ConnectionDecorator;
//import com.sas.custom.common.database.decorators.PreparedStatementDecorator;
import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public  class DatabaseTemplate_V2 implements  Runnable {
    private static final Logger logger = Logger.getLogger(DatabaseTemplate_V2.class);

    private Connection connection;
    private DataSource dataSource;

    private Long errorCode = 0L;
    private Long potentialErrorCode;
    private String errorDesc;

    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return connection.prepareStatement(sql);
    }
    public PreparedStatement createStatement(String statementStr) {
        try {
            return getConnection().prepareStatement(statementStr);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
//    public PreparedStatement createStatement(String statementStr) {
//        try {
//            return new PreparedStatementDecorator(getConnection().prepareStatement(statementStr), statementStr);
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
//    }

//    @Override
//    public CallableStatement createCallStatement(String statementStr) {
//        try {
//            return new CallableStatementDecorator(getConnection().prepareCall(statementStr), statementStr);
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
//    }
    //@Override
    public CallableStatement createCallStatement(String statementStr) {
        try {
            return  getConnection().prepareCall(statementStr);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void setMapJDBC(Map<String, DataSource> input) {
        dataSource = input.get("GeneralIO_Activity_Resource");
    }



    public void cleanUp()
    {
        try {
            if(connection != null) {
                connection.close();
            }
            connection = null;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        logger.trace(getClass().getName() + " : " + "Close connection from pool");
    }

    public Connection getConnection(DataSource pdataSource) {
        try {
            if(connection != null)
                return connection;
            //connection = new ConnectionDecorator(pdataSource.getConnection(), this.getClass());
            connection = pdataSource.getConnection();
            logger.trace(getClass().getName() + " : " + "Got connection from pool");
            return connection;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Connection getConnection() {

            if(connection != null)
                return connection;
            else
                throw new RuntimeException("No connection");

    }

    protected Long getDistinctCount(List<Object> list)
    {
        Long result = 0L;
        List<Object> counted = new LinkedList<>();
        for(Object o : list)
            if(!counted.contains(o))
            {
                ++result;
                counted.add(o);
            }
        return result;
    }

    public void setPotentialErrorCode(Long potentialErrorCode) {
        this.potentialErrorCode = potentialErrorCode;
    }

    public String getErrorDesc() {
        return errorDesc;
    }

    public Long getErrorCode() {
        return errorCode;
    }

    @Override
    public void run() {
        try {
            connection = getConnection(dataSource);
            execute();
            if(connection != null)
                connection.commit();
        } catch(SQLException e) {
            logger.error(e);
            errorCode = potentialErrorCode;
            errorDesc = e.getLocalizedMessage();
            if(connection != null)
                try {
                    connection.rollback();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
            if(potentialErrorCode == null)
                throw new RuntimeException(e);
        }finally {
            cleanUp();
        }
    }

    protected void setTransactional() {
        try {
            getConnection().setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    protected void execute() {
    }

//    public DataSource getDataSource() {
//        return dataSource;
//    }
}
