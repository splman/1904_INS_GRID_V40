package com.sas.custom.common.database.decorators;

import org.apache.log4j.Logger;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by y333325 on 2016-07-14.
 */
public class StatementLogger {
    private static final Logger logger = Logger.getLogger(StatementLogger.class);

    private String statement;
    private Map<Integer, Object> paramMap = new LinkedHashMap<>();

    protected void setStatement(String statement)
    {
        this.statement = statement;
    }

    protected void logStatement()
    {
        if(logger.isTraceEnabled())
            logger.trace("Executing statement=[" + statement + "] with params=[" + paramMap + "]");
    }

    protected void logParameter(Integer idx, Object value) {
        if(logger.isTraceEnabled())
            paramMap.put(idx, value);
    }
}
