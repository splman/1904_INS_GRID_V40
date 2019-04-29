package com.sas.custom.common.testUtils.mock.database;

import com.sas.analytics.ph.common.RTDMTable;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;

public class MockSqlStatement implements PreparedStatement, MockResultSetFactory {
    private String statement;
    private MockResultSetFactory mockResultSetFactory = this;

    MockSqlStatement(String statement) {
        this.statement = statement;
    }

    public ResultSet executeQuery() throws SQLException {
        return mockResultSetFactory.createResultSet();
    }

    public void setMockResultSetFactory(MockResultSetFactory mockResultSetFactory) {
        this.mockResultSetFactory = mockResultSetFactory;
    }

    public int executeUpdate() throws SQLException {
        return 0;
    }

    private void setParam(Object obj)
    {
        setParam(obj, false);
    }

    private void setParam(Object obj, boolean isString)
    {
        String objStr = isString ? "'" + obj.toString() + "'" : obj.toString();
        statement = statement.replaceFirst("[?]", obj == null ? "null" : objStr);
    }

    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        setParam("null");
    }

    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        setParam(x ? 1 : 0);
    }

    public void setByte(int parameterIndex, byte x) throws SQLException {
        setParam(x);
    }

    public void setShort(int parameterIndex, short x) throws SQLException {
        setParam(x);
    }

    public void setInt(int parameterIndex, int x) throws SQLException {
        setParam(x);
    }

    public void setLong(int parameterIndex, long x) throws SQLException {
        setParam(x);
    }

    public void setFloat(int parameterIndex, float x) throws SQLException {
        setParam(x);
    }

    public void setDouble(int parameterIndex, double x) throws SQLException {
        setParam(x);
    }

    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        setParam(x);
    }

    public void setString(int parameterIndex, String x) throws SQLException {
        setParam(x, true);
    }

    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        setParam(x);
    }

    public void setDate(int parameterIndex, Date x) throws SQLException {
        setParam(x);
    }

    public void setTime(int parameterIndex, Time x) throws SQLException {
        setParam(x);
    }

    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        setParam(x, true);
    }

    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setParam(x);
    }

    @Deprecated
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setParam(x);
    }

    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setParam(x);
    }

    public void clearParameters() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setObject(int parameterIndex, Object x) throws SQLException {
        setParam(x, x instanceof String);
    }

    public boolean execute() throws SQLException {
        return true;
    }

    public void addBatch() throws SQLException {

    }

    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        setParam(x);
    }

    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        setParam(x);
    }

    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        setParam(x, true);
    }

    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setURL(int parameterIndex, URL x) throws SQLException {
        setParam(x);
    }

    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        setParam(x);
    }

    public void setNString(int parameterIndex, String value) throws SQLException {
        setParam(value, true);
    }

    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        setParam(x);
    }

    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public ResultSet executeQuery(String sql) throws SQLException {
        return null;
    }

    public int executeUpdate(String sql) throws SQLException {
        return 0;
    }

    public void close() throws SQLException {

    }

    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    public void setMaxFieldSize(int max) throws SQLException {

    }

    public int getMaxRows() throws SQLException {
        return 0;
    }

    public void setMaxRows(int max) throws SQLException {

    }

    public void setEscapeProcessing(boolean enable) throws SQLException {

    }

    public int getQueryTimeout() throws SQLException {
        return 0;
    }

    public void setQueryTimeout(int seconds) throws SQLException {

    }

    public void cancel() throws SQLException {

    }

    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    public void clearWarnings() throws SQLException {

    }

    public void setCursorName(String name) throws SQLException {

    }

    public boolean execute(String sql) throws SQLException {
        return false;
    }

    public ResultSet getResultSet() throws SQLException {
        return mockResultSetFactory.createResultSet();
    }

    public int getUpdateCount() throws SQLException {
        return 0;
    }

    public boolean getMoreResults() throws SQLException {
        return false;
    }

    public void setFetchDirection(int direction) throws SQLException {

    }

    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_UNKNOWN;
    }

    public void setFetchSize(int rows) throws SQLException {

    }

    public int getFetchSize() throws SQLException {
        return 0;
    }

    public int getResultSetConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    public int getResultSetType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    public void addBatch(String sql) throws SQLException {

    }

    public void clearBatch() throws SQLException {

    }

    public int[] executeBatch() throws SQLException {
        return new int[0];
    }

    public Connection getConnection() throws SQLException {
        return null;
    }

    public boolean getMoreResults(int current) throws SQLException {
        return false;
    }

    public ResultSet getGeneratedKeys() throws SQLException {
        return null;
    }

    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return 0;
    }

    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return 0;
    }

    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return 0;
    }

    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return false;
    }

    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return false;
    }

    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return false;
    }

    public int getResultSetHoldability() throws SQLException {
        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {

    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {

    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    public String getStatement() {
        return statement;
    }

    @Override
    public ResultSet createResultSet() {
        return new MockResultSet(new RTDMTable());
    }
}
