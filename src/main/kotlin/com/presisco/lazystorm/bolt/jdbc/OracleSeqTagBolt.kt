package com.presisco.lazystorm.bolt.jdbc

import com.presisco.lazyjdbc.client.OracleMapJdbcClient
import com.presisco.lazystorm.addFieldToNewMap
import com.presisco.lazystorm.getString
import org.apache.storm.topology.BasicOutputCollector

class OracleSeqTagBolt : JdbcClientBolt<OracleMapJdbcClient>() {
    private lateinit var tag: String

    override fun loadJdbcClient() = OracleMapJdbcClient(dataSource, queryTimeout, rollbackOnBatchFailure)

    override fun configure(config: Map<String, *>) {
        super.configure(config)
        tag = config.getString("tag")
    }

    override fun process(
            boltName: String,
            streamName: String,
            data: List<*>,
            table: String,
            client: OracleMapJdbcClient,
            collector: BasicOutputCollector
    ) = client.querySequence(table, data.size)
            .mapIndexed { index, id ->
                (data[index] as Map<String, Any?>).addFieldToNewMap(tag to id)
            }

}