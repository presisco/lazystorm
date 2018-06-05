package com.presisco.lazystorm.bolt.jdbc

import org.apache.storm.topology.OutputFieldsDeclarer
import org.slf4j.LoggerFactory

class BatchMapReplaceJdbcBolt : BatchMapJdbcBolt() {
    private val logger = LoggerFactory.getLogger(BatchMapReplaceJdbcBolt::class.java)

    init {
        setOnBatchFullCallback { batch ->
            mapJdbcClient.replace(tableName, batch)
        }
    }

    override fun declareOutputFields(declarer: OutputFieldsDeclarer) {

    }
}