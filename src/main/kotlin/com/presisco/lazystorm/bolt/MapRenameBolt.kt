package com.presisco.lazystorm.bolt

import org.apache.storm.topology.BasicOutputCollector
import org.apache.storm.topology.FailedException
import org.apache.storm.tuple.Tuple
import org.apache.storm.tuple.Values
import org.slf4j.LoggerFactory

class MapRenameBolt(
        private val renameMap: HashMap<String, String>
) : LazyBasicBolt<Any>() {
    private val logger = LoggerFactory.getLogger(MapRenameBolt::class.java)

    init {
        val intersection = renameMap.keys.intersect(renameMap.values)
        if (intersection.isNotEmpty()) {
            throw FailedException("intersection in rename map: $intersection")
        }
    }

    private fun renameMap(map: MutableMap<String, Any?>) {
        renameMap.forEach { original, renamed ->
            map[renamed] = map[original]
            map.remove(original)
        }
    }

    override fun execute(tuple: Tuple, basicOutputCollector: BasicOutputCollector) {
        val data = getArrayListInput(tuple)

        data.forEach { map -> renameMap(map as MutableMap<String, Any?>) }

        basicOutputCollector.emit(Values(data))
    }

}