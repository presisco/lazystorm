package com.presisco.lazystorm.bolt.jdbc

import org.apache.storm.topology.BasicOutputCollector
import org.apache.storm.topology.FailedException
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.tuple.Tuple
import org.slf4j.LoggerFactory
import java.sql.SQLException
import javax.sql.DataSource

class MapInsertJdbcBolt(
        dataSource: DataSource,
        tableName: String,
        queryTimeout: Int = 2,
        rollbackOnBatchFailure: Boolean = true
): MapJdbcBolt(
        dataSource,
        tableName,
        queryTimeout,
        rollbackOnBatchFailure
) {
    private val logger = LoggerFactory.getLogger(MapInsertJdbcBolt::class.java)
    private var srcPos: Int = 0
    private var srcField: String = ""

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

    override fun execute(tuple: Tuple, basicOutputCollector: BasicOutputCollector) {
        val data = getData(tuple)

        try {
            when (data) {
                is List<*> -> mapJdbcClient.insert(tableName, data as List<Map<String, Any?>>)
                is Map<*, *> -> mapJdbcClient.insert(tableName, arrayListOf(data as Map<String, Any?>))
                else -> throw FailedException("unsupported type of data: ${data::class.java.simpleName}")
            }
        }catch (e: SQLException){
            throw FailedException("sql execution error on table: $tableName", e)
        }
    }

    override fun declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer) {

    }
}