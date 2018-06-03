package com.presisco.lazystorm.bolt.jdbc

import com.presisco.lazystorm.bolt.Constants
import org.apache.storm.topology.BasicOutputCollector
import org.apache.storm.topology.FailedException
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.tuple.Tuple
import org.slf4j.LoggerFactory
import java.sql.SQLException
import javax.sql.DataSource

class MapInsertJdbcBolt(
        srcPos: Int = Constants.DATA_FIELD_POS,
        srcField: String = Constants.DATA_FIELD_NAME,
        dataSource: DataSource,
        tableName: String,
        queryTimeout: Int = 2,
        rollbackOnBatchFailure: Boolean = true
) : MapJdbcBolt<Any>(
        srcPos,
        srcField,
        dataSource,
        tableName,
        queryTimeout,
        rollbackOnBatchFailure
) {
    private val logger = LoggerFactory.getLogger(MapInsertJdbcBolt::class.java)

    override fun execute(tuple: Tuple, basicOutputCollector: BasicOutputCollector) {
        val data = getInput(tuple)

        try {
            when (data) {
                is List<*> -> mapJdbcClient.insert(tableName, data)
                is Map<*, *> -> mapJdbcClient.insert(tableName, arrayListOf(data))
                else -> throw FailedException("unsupported type of data: ${data::class.java.simpleName}")
            }
        }catch (e: SQLException){
            throw FailedException("sql execution error on table: $tableName", e)
        }
    }

    override fun declareOutputFields(declarer: OutputFieldsDeclarer) {

    }
}