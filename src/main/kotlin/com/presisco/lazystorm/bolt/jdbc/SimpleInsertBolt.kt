package com.presisco.lazystorm.bolt.jdbc

import com.presisco.lazyjdbc.client.MapJdbcClient
import org.apache.storm.topology.BasicOutputCollector
import org.slf4j.LoggerFactory

open class SimpleInsertBolt : MapJdbcClientBolt() {
    private val logger = LoggerFactory.getLogger(SimpleInsertBolt::class.java)

    override fun process(
            boltName: String,
            streamName: String,
            data: List<*>,
            table: String,
            client: MapJdbcClient,
            collector: BasicOutputCollector
    ) = client.insert(table, data as List<Map<String, Any?>>).map { data[it] }

}