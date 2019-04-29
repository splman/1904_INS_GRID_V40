package com.sas.custom.common.testUtils;


        import com.sas.analytics.ph.common.RTDMTable;
        import com.sas.analytics.ph.common.jaxb.DataTypes;
        import com.sas.custom.common.grid.GridColumnDefinition;

        import java.sql.Timestamp;
        import java.util.*;

public class RTDMTableTestUtils {
    public static Calendar currentCal;

    static {
        currentCal = Calendar.getInstance();
        Date date = new Date();
        date.setTime(1000000000000L);
        currentCal.setTime(date);
    }

    public static String getCurrentCalStr() {
        return new Timestamp(((Calendar) currentCal).getTimeInMillis()).toString();
    }

    public static final String STRING_COL_NAME = "TEST_STRING_COL";
    public static final String LONG_COL_NAME = "TEST_LONG_COL";
    public static final String DOUBLE_COL_NAME = "TEST_DOUBLE_COL";
    public static final String DATE_COL_NAME = "TEST_DATE_COL";
    public static final String BOOLEAN_COL_NAME = "TEST_BOOLEAN_COL";

    public static final int GRID_ROW_CNT = 4;
    public static final String[] STRING_VALUES = {"ABC", "", "  ĄŹ  ", null};
    public static  Long[] LONG_VALUES = {4144141234L, 0L, null, -5L};
    public static final Double[] DOUBLE_VALUES = {412341323.22, null, -2.123123123, 0.0};
    public static final Calendar[] DATE_VALUES = {null, currentCal, null, currentCal};
    public static final Boolean[] BOOLEAN_VALUES = {false, null, true, null};

    public static final List<GridColumnDefinition> TEST_COLUMNS = Arrays.asList(
            new GridColumnDefinition(STRING_COL_NAME, DataTypes.STRING),
            new GridColumnDefinition(LONG_COL_NAME, DataTypes.INT),
            new GridColumnDefinition(DOUBLE_COL_NAME, DataTypes.FLOAT),
            new GridColumnDefinition(DATE_COL_NAME, DataTypes.DATETIME),
            new GridColumnDefinition(BOOLEAN_COL_NAME, DataTypes.BOOLEAN)
    );

    public static RTDMTable createTestRTDMTable() {
        return createTestRTDMTable(TEST_COLUMNS);
    }

    public static RTDMTable createTestRTDMTable(List<GridColumnDefinition> columns) {
        RTDMTable rtdmTable = new RTDMTable();
        for(GridColumnDefinition column : columns) {
            switch (column.getType()) {
                case STRING:
                    rtdmTable.columnAdd(column.getName(), STRING_VALUES);
                    break;
                case INT:
                    rtdmTable.columnAdd(column.getName(), LONG_VALUES);
                    break;
                case FLOAT:
                    rtdmTable.columnAdd(column.getName(), DOUBLE_VALUES);
                    break;
                case DATETIME:
                    rtdmTable.columnAdd(column.getName(), DATE_VALUES);
                    break;
                case BOOLEAN:
                    rtdmTable.columnAdd(column.getName(), BOOLEAN_VALUES);
                    break;
            }
        }
        return rtdmTable;
    }
}
