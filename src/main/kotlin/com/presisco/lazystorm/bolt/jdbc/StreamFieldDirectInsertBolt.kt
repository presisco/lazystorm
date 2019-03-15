package com.presisco.lazystorm.bolt.jdbc

import com.presisco.lazyjdbc.client.MapJdbcClient
import org.apache.storm.topology.BasicOutputCollector

class StreamFieldDirectInsertBolt(
        private val streamField: String
): MapJdbcClientBolt() {
    override fun process(
            boltName: String,
            streamName: String,
            data: List<*>,
            table: String,
            client: MapJdbcClient,
            collector: BasicOutputCollector
    ): List<*> {
        val taggedList = data.map {
            original ->
            val tagged = hashMapOf<String, Any?>()
            tagged.putAll(original as Map<String, *>)
            tagged[streamField] = streamName
            tagged
        }
        val failedSet = client.insert(table, taggedList)
        return if (failedSet.isEmpty()) {
            listOf<Any>()
        } else {
            data.filterIndexed { index, _ -> failedSet.contains(index) }
        }
    }
}