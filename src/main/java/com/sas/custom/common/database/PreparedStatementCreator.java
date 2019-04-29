package com.sas.custom.common.database;


        import java.sql.CallableStatement;
        import java.sql.PreparedStatement;

public interface PreparedStatementCreator {
    PreparedStatement createStatement(String statementStr);

    CallableStatement createCallStatement(String statementStr);
}