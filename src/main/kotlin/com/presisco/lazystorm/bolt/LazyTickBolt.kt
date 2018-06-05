package com.presisco.lazystorm.bolt

import org.apache.storm.Config
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseTickTupleAwareRichBolt
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.slf4j.LoggerFactory

abstract class LazyTickBolt<out T>(
        private var srcPos: Int = Constants.DATA_FIELD_POS,
        private var srcField: String = Constants.DATA_FIELD_NAME,
        private var tickIntervalSec: Int = 60
) : BaseTickTupleAwareRichBolt() {
    private val logger = LoggerFactory.getLogger(LazyTickBolt::class.java)

    fun setSrcPos(pos: Int): LazyTickBolt<T> {
        srcPos = pos
        return this
    }

    fun setSrcField(field: String): LazyTickBolt<T> {
        srcField = field
        return this
    }

    open fun setTickIntervalSec(intervalSec: Int): LazyTickBolt<T> {
        tickIntervalSec = intervalSec
        return this
    }

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