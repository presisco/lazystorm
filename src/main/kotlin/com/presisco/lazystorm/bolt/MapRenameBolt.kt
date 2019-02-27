package com.presisco.lazystorm.bolt

import org.apache.storm.topology.BasicOutputCollector
import org.apache.storm.topology.FailedException
import org.apache.storm.tuple.Tuple
import org.apache.storm.tuple.Values
import org.slf4j.LoggerFactory

/**
 * ！！！！！重要！！！！！
 * 对数据做的修改一定要新建Map保存，否则会导致bolt之间的数据流问题
 */
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

    private fun renameMap(map: MutableMap<String, Any?>): HashMap<String, Any?> {
        val renamedMap = hashMapOf<String, Any?>()
        map.forEach { key, value ->
            if (renameMap.containsKey(key)) {
                renamedMap[renameMap[key]!!] = value
            } else {
                renamedMap[key] = value
            }
        }
        return renamedMap
    }

    override fun execute(tuple: Tuple, basicOutputCollector: BasicOutputCollector) {
        val data = getArrayListInput(tuple)

        val renamedList = data.map { map -> renameMap(map as MutableMap<String, Any?>) }

        basicOutputCollector.emit(Values(renamedList))
    }

}