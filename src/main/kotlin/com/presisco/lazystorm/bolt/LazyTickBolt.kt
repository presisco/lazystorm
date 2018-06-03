package com.presisco.lazystorm.bolt

import org.apache.storm.Config
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseTickTupleAwareRichBolt
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.slf4j.LoggerFactory

abstract class LazyTickBolt<out T>(
        private val srcPos: Int = Constants.DATA_FIELD_POS,
        private val srcField: String = Constants.DATA_FIELD_NAME,
        private val tickIntervalSec: Int
) : BaseTickTupleAwareRichBolt() {
    private val logger = LoggerFactory.getLogger(LazyTickBolt::class.java)

    fun getInput(tuple: Tuple) = if (srcPos != Constants.DATA_FIELD_POS)
        tuple.getValue(srcPos) as T
    else
        tuple.getValueByField(srcField) as T

    override fun getComponentConfiguration(): MutableMap<String, Any> {
        return mutableMapOf(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS to tickIntervalSec)
    }

    override fun declareOutputFields(declarer: OutputFieldsDeclarer) {
        declarer.declare(Fields(Constants.DATA_FIELD_NAME))
    }
}