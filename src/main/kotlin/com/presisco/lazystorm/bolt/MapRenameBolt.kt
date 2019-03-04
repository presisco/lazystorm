package com.presisco.lazystorm.bolt

import org.apache.storm.topology.FailedException
import org.slf4j.LoggerFactory

/**
 * ！！！！！重要！！！！！
 * 对数据做的修改一定要新建Map保存，否则会导致bolt之间的数据流问题
 */
class MapRenameBolt(
        private val renameMap: HashMap<String, String>
) : MapOpBolt() {
    private val logger = LoggerFactory.getLogger(MapRenameBolt::class.java)

    init {
        val intersection = renameMap.keys.intersect(renameMap.values)
        if (intersection.isNotEmpty()) {
            throw FailedException("intersection in rename map: $intersection")
        }
    }

    override fun operate(input: Map<String, *>): HashMap<String, *> {
        val renamedMap = hashMapOf<String, Any?>()
        input.forEach { key, value ->
            if (renameMap.containsKey(key)) {
                renamedMap[renameMap[key]!!] = value
            } else {
                renamedMap[key] = value
            }
        }
        return renamedMap
    }


}