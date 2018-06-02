package com.presisco.lazystorm.bolt.jdbc

import org.apache.storm.Config
import org.apache.storm.task.OutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.base.BaseTickTupleAwareRichBolt
import javax.sql.DataSource

abstract class BatchJdbcBolt(
        protected val dataSource: DataSource,
        protected val tableName: String,
        protected val queryTimeout: Int = 2,
        protected val rollbackOnBatchFailure: Boolean = true,
        protected val ack: Boolean = true,
        protected val tickIntervalSec: Int = 5
): BaseTickTupleAwareRichBolt(){
    protected lateinit var outputCollector: OutputCollector

    override fun getComponentConfiguration(): MutableMap<String, Any> {
        return mutableMapOf(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS to tickIntervalSec)
    }

    override fun prepare(stormConfig: MutableMap<*, *>, context: TopologyContext, outputCollector: OutputCollector) {
        this.outputCollector = outputCollector
    }
}