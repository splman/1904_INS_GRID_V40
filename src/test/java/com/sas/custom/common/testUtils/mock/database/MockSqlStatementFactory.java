package com.sas.custom.common.testUtils.mock.database;

public interface MockSqlStatementFactory
{
    MockSqlStatement createMockSqlCallableStatement(String sql);
}