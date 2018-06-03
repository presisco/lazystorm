package com.presisco.lazystorm.bolt.jdbc

import com.presisco.lazystorm.bolt.Constants
import org.apache.storm.task.OutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.FailedException
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.tuple.Tuple
import org.slf4j.LoggerFactory
import java.sql.SQLException
import java.util.concurrent.ArrayBlockingQueue
import javax.sql.DataSource

open class BatchMapInsertJdbcBolt(
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
){
    private val logger = LoggerFactory.getLogger(BatchMapInsertJdbcBolt::class.java)

    @Transient
    protected lateinit var mapJdbcClient: MapJdbcClient

    init {
        setOnBatchFullCallback { batch ->
            mapJdbcClient.insert(tableName, batch)
        }
    }

    override fun prepare(stormConfig: MutableMap<*, *>, context: TopologyContext, outputCollector: OutputCollector) {
        super.prepare(stormConfig, context, outputCollector)
        mapJdbcClient = MapJdbcClient(dataSource, queryTimeout, rollbackOnBatchFailure)
    }

    override fun declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer) {

    }
}