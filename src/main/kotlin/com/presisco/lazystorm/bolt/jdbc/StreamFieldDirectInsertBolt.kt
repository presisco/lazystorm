package com.presisco.lazystorm.bolt.jdbc

import com.presisco.lazyjdbc.client.MapJdbcClient
import com.presisco.lazystorm.addFieldToNewMap
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
    ) = client.insert(
            table,
            data.map { (it as Map<String, Any?>).addFieldToNewMap(streamField to streamName) }
    ).map { data[it] }

}