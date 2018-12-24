package com.presisco.lazystorm.bolt.jdbc

import com.presisco.lazyjdbc.client.OracleMapJdbcClient

class OracleSeqTagBolt(
        private val sequence: String,
        private val tag: String
) : JdbcClientBolt<OracleMapJdbcClient>() {

    override fun loadJdbcClient() = OracleMapJdbcClient(dataSource, queryTimeout, rollbackOnBatchFailure)

    override fun process(data: List<*>, client: OracleMapJdbcClient): List<*> {
        val ids = client.querySequence(sequence, data.size)
        data.forEachIndexed { index, item -> (item as MutableMap<String, Any?>)[tag] = ids[index] }
        return data
    }
}