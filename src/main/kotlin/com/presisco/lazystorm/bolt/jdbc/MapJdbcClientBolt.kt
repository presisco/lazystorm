package com.presisco.lazystorm.bolt.jdbc

import com.presisco.lazyjdbc.client.MapJdbcClient
import com.presisco.lazystorm.defaultTimeStampFormat

abstract class MapJdbcClientBolt: JdbcClientBolt<MapJdbcClient>() {

    override fun loadJdbcClient() = MapJdbcClient(dataSource, queryTimeout, rollbackOnBatchFailure)
            .withDateFormat(defaultTimeStampFormat)

}