package com.presisco.lazystorm.bolt.jdbc

import com.presisco.lazyjdbc.client.MapJdbcClient
import org.slf4j.LoggerFactory

class SimpleReplaceBolt : JdbcClientBolt<MapJdbcClient>() {
    private val logger = LoggerFactory.getLogger(SimpleReplaceBolt::class.java)

    override fun loadJdbcClient() = MapJdbcClient(dataSource, queryTimeout, rollbackOnBatchFailure)

    override fun process(data: List<*>, table: String, client: MapJdbcClient): List<*> {
        val failedSet = client.replace(table, data as List<Map<String, Any?>>)
        return if (failedSet.isEmpty()) {
            listOf<Any>()
        } else {
            data.filterIndexed { index, _ -> failedSet.contains(index) }
        }
    }
}