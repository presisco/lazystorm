package com.presisco.lazystorm.bolt.jdbc

import com.presisco.lazyjdbc.client.OracleMapJdbcClient
import com.presisco.lazystorm.addFieldToNewMap
import org.apache.storm.topology.BasicOutputCollector

class OracleSeqTagBolt(
        private val tag: String
) : JdbcClientBolt<OracleMapJdbcClient>() {

    override fun loadJdbcClient() = OracleMapJdbcClient(dataSource, queryTimeout, rollbackOnBatchFailure)

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