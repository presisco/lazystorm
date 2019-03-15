package com.presisco.lazystorm.bolt.jdbc

import com.presisco.lazyjdbc.client.MapJdbcClient
import com.presisco.lazystorm.bolt.Constants
import org.apache.storm.topology.BasicOutputCollector
import org.slf4j.LoggerFactory

open class SimpleInsertBolt : JdbcClientBolt<MapJdbcClient>() {
    private val logger = LoggerFactory.getLogger(SimpleInsertBolt::class.java)

    override fun loadJdbcClient() = MapJdbcClient(dataSource, queryTimeout, rollbackOnBatchFailure).withDateFormat(Constants.timeStampFormat)

    override fun process(data: List<*>, table: String, client: MapJdbcClient, collector: BasicOutputCollector): List<*> {
        val failedSet = client.insert(table, data as List<Map<String, Any?>>)
        return if (failedSet.isEmpty()) {
            listOf<Any>()
        } else {
            data.filterIndexed { index, _ -> failedSet.contains(index) }
        }
    }
}