package com.presisco.lazystorm.bolt.jdbc

import com.presisco.lazyjdbc.client.MapJdbcClient
import org.slf4j.LoggerFactory

class SimpleInsertBolt : JdbcClientBolt<MapJdbcClient>() {
    private val logger = LoggerFactory.getLogger(SimpleInsertBolt::class.java)

    override fun loadJdbcClient() = MapJdbcClient(dataSource, queryTimeout, rollbackOnBatchFailure)

    override fun process(data: List<*>, client: MapJdbcClient): List<*> {
        val failedSet = client.insert(tableName, data as List<Map<String, Any?>>)
        return if (failedSet.isEmpty()) {
            listOf<Any>()
        } else {
            data.filterIndexed { index, _ -> failedSet.contains(index) }
        }
    }
}