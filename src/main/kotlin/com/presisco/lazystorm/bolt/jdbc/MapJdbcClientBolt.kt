package com.presisco.lazystorm.bolt.jdbc

import com.presisco.lazyjdbc.client.MapJdbcClient
import com.presisco.lazystorm.DEFAULT_TIME_FORMAT
import java.time.format.DateTimeFormatter

abstract class MapJdbcClientBolt: JdbcClientBolt<MapJdbcClient>() {
    private var timeFormatString: String = DEFAULT_TIME_FORMAT

    fun setTimeFormat(format: String): MapJdbcClientBolt {
        timeFormatString = format
        return this
    }

    override fun loadJdbcClient() = MapJdbcClient(dataSource, queryTimeout, rollbackOnBatchFailure)
        .withDateFormat(DateTimeFormatter.ofPattern(timeFormatString))

}