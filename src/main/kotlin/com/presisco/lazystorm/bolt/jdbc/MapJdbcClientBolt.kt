package com.presisco.lazystorm.bolt.jdbc

import com.presisco.lazyjdbc.client.MapJdbcClient
import com.presisco.lazystorm.bolt.Constants

abstract class MapJdbcClientBolt: JdbcClientBolt<MapJdbcClient>() {

    override fun loadJdbcClient() = com.presisco.lazyjdbc.client.MapJdbcClient(dataSource, queryTimeout, rollbackOnBatchFailure).withDateFormat(Constants.timeStampFormat)

}