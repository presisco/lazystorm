package com.presisco.lazystorm.bolt

import org.apache.storm.topology.BasicOutputCollector
import org.apache.storm.tuple.Tuple
import org.apache.storm.tuple.Values
import org.slf4j.LoggerFactory

class MapStripBolt(
        private val stripList: ArrayList<String>
) : LazyBasicBolt<Any>() {
    private val logger = LoggerFactory.getLogger(MapRenameBolt::class.java)

    private fun stripMap(map: MutableMap<String, Any?>) = stripList.forEach { map.remove(it) }

    override fun execute(tuple: Tuple, basicOutputCollector: BasicOutputCollector) {
        val data = getArrayListInput(tuple)

        data.forEach { map -> stripMap(map as MutableMap<String, Any?>) }

        basicOutputCollector.emit(Values(data))
    }

}