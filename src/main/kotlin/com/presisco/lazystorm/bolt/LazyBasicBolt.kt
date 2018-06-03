package com.presisco.lazystorm.bolt

import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseBasicBolt
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.slf4j.LoggerFactory

abstract class LazyBasicBolt<out T>(
        private var srcPos: Int = Constants.DATA_FIELD_POS,
        private var srcField: String = Constants.DATA_FIELD_NAME
) : BaseBasicBolt() {
    private val logger = LoggerFactory.getLogger(LazyBasicBolt::class.java)

    fun setSrcPos(pos: Int): LazyBasicBolt<T> {
        srcPos = pos
        return this
    }

    fun setSrcField(field: String): LazyBasicBolt<T> {
        srcField = field
        return this
    }
    
    fun getInput(tuple: Tuple) = if (srcPos != Constants.DATA_FIELD_POS)
        tuple.getValue(srcPos) as T
    else
        tuple.getValueByField(srcField) as T

    override fun declareOutputFields(declarer: OutputFieldsDeclarer) {
        declarer.declare(Fields(Constants.DATA_FIELD_NAME))
    }
}