package com.presisco.lazystorm.bolt.jdbc

import org.apache.storm.topology.OutputFieldsDeclarer
import org.slf4j.LoggerFactory

open class BatchMapInsertJdbcBolt : BatchMapJdbcBolt() {
    private val logger = LoggerFactory.getLogger(BatchMapInsertJdbcBolt::class.java)

    init {
        setOnBatchFullCallback { batch ->
            mapJdbcClient.insert(tableName, batch)
        }
    }

    override fun declareOutputFields(declarer: OutputFieldsDeclarer) {

    }
}