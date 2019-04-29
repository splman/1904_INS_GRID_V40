package com.sas.custom.common.grid

import com.sas.analytics.ph.common.RTDMTable
import com.sas.custom.common.testUtils.RTDMTableTestUtils

/**
 * Created by y333325 on 2016-07-26.
 */
class RTDMTableUtilsTest extends GroovyTestCase {
    public void testAddColumnToGridLong() {
        def columnName = 'LONG_LIST'
        def list = [1l, 2l, 3l, 4l]
        def inputGrid = RTDMTableTestUtils.createTestRTDMTable()
        def outputGrid = new RTDMTable(inputGrid)

        inputGrid.columnAdd(columnName, (Long[]) list.toArray())
        RTDMTableUtils.addColumnToGrid(outputGrid, list, columnName)
        assertEquals(outputGrid, inputGrid)
    }

}
