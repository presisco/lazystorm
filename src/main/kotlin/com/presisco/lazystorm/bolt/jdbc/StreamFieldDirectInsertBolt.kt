package com.presisco.lazystorm.bolt.jdbc

import com.presisco.lazyjdbc.client.MapJdbcClient
import com.presisco.lazystorm.addFieldToNewMap
import com.presisco.lazystorm.getString
import org.apache.storm.topology.BasicOutputCollector

class StreamFieldDirectInsertBolt : MapJdbcClientBolt() {
    private lateinit var streamField: String

    override fun configure(config: Map<String, *>) {
        super.configure(config)
        streamField = config.getString("field")
    }

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