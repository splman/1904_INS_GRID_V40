package com.sas.custom.common.database;

import com.sas.analytics.ph.common.jaxb.DataTypes;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;

/**
 * Created by Y344104 on 2016-08-24.
 */
public class DatabaseUtils {
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
            preparedStatement.setNull(idx, DatabaseUtils.getSQLType(type));
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
}
