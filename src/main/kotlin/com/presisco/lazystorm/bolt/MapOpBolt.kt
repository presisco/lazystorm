package com.presisco.lazystorm.bolt

import org.apache.storm.topology.BasicOutputCollector
import org.apache.storm.tuple.Tuple
import org.slf4j.LoggerFactory

abstract class MapOpBolt : LazyBasicBolt<Any>() {
    private val logger = LoggerFactory.getLogger(MapOpBolt::class.java)

    abstract fun operate(input: Map<String, *>): HashMap<String, *>

    override fun execute(tuple: Tuple, outputCollector: BasicOutputCollector) {
        val data = getArrayListInput(tuple)

        val outputList = data.map { operate(it as Map<String, Any?>) }

        outputCollector.emitDataToStreams(tuple.sourceStreamId, outputList)
    }
}