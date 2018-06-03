package com.presisco.lazystorm.bolt.jdbc

import com.presisco.lazyjdbc.client.MapJdbcClient
import com.presisco.lazystorm.bolt.Constants
import org.apache.storm.task.OutputCollector
import org.apache.storm.task.TopologyContext
import org.slf4j.LoggerFactory
import javax.sql.DataSource

abstract class BatchMapJdbcBolt(
        srcPos: Int = Constants.DATA_FIELD_POS,
        srcField: String = Constants.DATA_FIELD_NAME,
        dataSource: DataSource,
        tableName: String,
        batchSize: Int = 1000,
        queryTimeout: Int = 2,
        rollbackOnBatchFailure: Boolean = true,
        ack: Boolean = true,
        tickIntervalSec: Int = 5
) : BatchJdbcBolt<Map<String, *>>(
        srcPos,
        srcField,
        dataSource,
        tableName,
        batchSize,
        queryTimeout,
        rollbackOnBatchFailure,
        ack,
        tickIntervalSec
) {
    private val logger = LoggerFactory.getLogger(BatchMapJdbcBolt::class.java)

    @Transient
    protected lateinit var mapJdbcClient: MapJdbcClient

    override fun prepare(stormConfig: MutableMap<*, *>, context: TopologyContext, outputCollector: OutputCollector) {
        super.prepare(stormConfig, context, outputCollector)
        mapJdbcClient = MapJdbcClient(dataSource, queryTimeout, rollbackOnBatchFailure)
    }

}