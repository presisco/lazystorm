package com.presisco.lazystorm.bolt.jdbc

import com.presisco.lazyjdbc.client.MapJdbcClient
import org.apache.storm.task.TopologyContext
import org.slf4j.LoggerFactory

abstract class MapJdbcBolt<T> : BaseJdbcBolt<T>() {
    private val logger = LoggerFactory.getLogger(MapJdbcBolt::class.java)

    @Transient
    protected lateinit var mapJdbcClient: MapJdbcClient

    override fun prepare(stormConf: MutableMap<Any?, Any?>, context: TopologyContext) {
        super.prepare(stormConf, context)
        try {
            mapJdbcClient = MapJdbcClient(dataSource, queryTimeout, rollbackOnBatchFailure)
        } catch (e: Exception) {
            throw IllegalStateException("get connection failed! message: ${e.message}, data source name: ${dataSourceLoader.name}, config: ${dataSourceLoader.config}")
        }
    }
}