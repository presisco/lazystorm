package com.presisco.lazystorm.bolt.jdbc

import com.presisco.lazyjdbc.client.MapJdbcClient
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
        dataSource: DataSource,
        tableName: String,
        queryTimeout: Int = 2,
        rollbackOnBatchFailure: Boolean = true,
        ack: Boolean = true,
        tickIntervalSec: Int = 5
): BatchJdbcBolt(
        dataSource,
        tableName,
        queryTimeout,
        rollbackOnBatchFailure,
        ack,
        tickIntervalSec
){
    private val logger = LoggerFactory.getLogger(BatchMapInsertJdbcBolt::class.java)
    private var srcPos: Int = 0
    private var srcField: String = ""

    @Transient
    protected lateinit var mapJdbcClient: MapJdbcClient

    private lateinit var insertQueue: ArrayBlockingQueue<Map<String, *>>

    override fun prepare(stormConfig: MutableMap<*, *>, context: TopologyContext, outputCollector: OutputCollector) {
        super.prepare(stormConfig, context, outputCollector)
        mapJdbcClient = MapJdbcClient(dataSource, queryTimeout, rollbackOnBatchFailure)
    }

    fun setDataPosition(pos: Int){
        srcPos = pos
    }

    fun setDataField(field: String){
        srcField = field
    }

    fun getData(tuple: Tuple) = if (srcField != "")
        tuple.getValueByField(srcField)
    else if (srcPos >= 0)
        tuple.getValue(srcPos)
    else {
        logger.debug("invalid src params! try getting from pos: 0")
        tuple.getValue(0)
    }

    override fun process(tuple: Tuple) {
        val data = getData(tuple)

        try {
            when (data) {
                is List<*> -> insertQueue.addAll(data as List<Map<String, *>>)
                is Map<*, *> -> insertQueue.add(data as Map<String, *>)
                else -> throw FailedException("unsupported type of data: ${data::class.java.simpleName}")
            }
            outputCollector.ack(tuple)
        }catch (e: Exception){
            outputCollector.reportError(e)
            outputCollector.fail(tuple)
        }
    }

    override fun onTickTuple(tuple: Tuple?) {
        val insertList = insertQueue.take(insertQueue.size)
        try {
            mapJdbcClient.insert(tableName, insertList)
        }catch (e: SQLException){
            throw FailedException("sql execution error on table: $tableName", e)
        }
    }

    override fun declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer) {

    }
}