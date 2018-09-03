package com.presisco.lazystorm.bolt.jdbc

import com.presisco.lazyjdbc.client.MapJdbcClient
import org.apache.storm.task.OutputCollector
import org.apache.storm.task.TopologyContext
import org.slf4j.LoggerFactory

abstract class BatchMapJdbcBolt : BatchJdbcBolt<Map<String, *>>() {
    private val logger = LoggerFactory.getLogger(BatchMapJdbcBolt::class.java)

    @Transient
    protected lateinit var mapJdbcClient: MapJdbcClient

    override fun prepare(stormConfig: MutableMap<*, *>, context: TopologyContext, outputCollector: OutputCollector) {
        super.prepare(stormConfig, context, outputCollector)
        try {
            mapJdbcClient = MapJdbcClient(dataSource, queryTimeout, rollbackOnBatchFailure)
        } catch (e: Exception) {
            throw IllegalStateException("get connection failed! message: ${e.message}, data source name: ${dataSourceHolder.name}, config: ${dataSourceHolder.config}")
        }
    }

}