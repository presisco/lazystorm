package com.presisco.lazystorm.bolt.jdbc

import com.presisco.lazyjdbc.client.MapJdbcClient
import org.apache.storm.topology.OutputFieldsDeclarer
import org.slf4j.LoggerFactory
import javax.sql.DataSource

open class BatchMapInsertJdbcBolt(
        dataSource: DataSource,
        tableName: String,
        batchSize: Int = 1000,
        queryTimeout: Int = 2,
        rollbackOnBatchFailure: Boolean = true,
        ack: Boolean = true,
        tickIntervalSec: Int = 5
) : BatchJdbcBolt<Map<String, *>>(
        dataSource,
        tableName,
        batchSize,
        queryTimeout,
        rollbackOnBatchFailure,
        ack,
        tickIntervalSec
){
    private val logger = LoggerFactory.getLogger(BatchMapInsertJdbcBolt::class.java)

    @Transient
    protected lateinit var mapJdbcClient: MapJdbcClient

    init {
        setOnBatchFullCallback { batch ->
            mapJdbcClient.insert(tableName, batch)
        }
    }

    override fun declareOutputFields(declarer: OutputFieldsDeclarer) {

    }
}