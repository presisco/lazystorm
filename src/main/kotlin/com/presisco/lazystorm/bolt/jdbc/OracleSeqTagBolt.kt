package com.presisco.lazystorm.bolt.jdbc

import com.presisco.lazyjdbc.client.OracleMapJdbcClient
import org.apache.storm.topology.BasicOutputCollector

class OracleSeqTagBolt(
        private val tag: String
) : JdbcClientBolt<OracleMapJdbcClient>() {

    override fun loadJdbcClient() = OracleMapJdbcClient(dataSource, queryTimeout, rollbackOnBatchFailure)

    override fun process(data: List<*>, table: String, client: OracleMapJdbcClient, collector: BasicOutputCollector): List<*> {
        val ids = client.querySequence(table, data.size)
        data.forEachIndexed { index, item -> (item as MutableMap<String, Any?>)[tag] = ids[index] }
        return data
    }
}