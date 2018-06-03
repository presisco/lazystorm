package com.presisco.lazystorm.bolt.jdbc

import org.apache.storm.task.TopologyContext
import org.slf4j.LoggerFactory
import javax.sql.DataSource

abstract class MapJdbcBolt<out T>(
        srcPos: Int,
        srcField: String,
        dataSource: DataSource,
        tableName: String,
        queryTimeout: Int = 2,
        rollbackOnBatchFailure: Boolean = true
) : BaseJdbcBolt<T>(
        srcPos,
        srcField,
        dataSource,
        tableName,
        queryTimeout,
        rollbackOnBatchFailure
) {
    private val logger = LoggerFactory.getLogger(MapJdbcBolt::class.java)

    @Transient
    protected lateinit var mapJdbcClient: MapJdbcClient

    override fun prepare(stormConf: MutableMap<Any?, Any?>?, context: TopologyContext?) {
        mapJdbcClient = MapJdbcClient(dataSource, queryTimeout, rollbackOnBatchFailure)
    }
}