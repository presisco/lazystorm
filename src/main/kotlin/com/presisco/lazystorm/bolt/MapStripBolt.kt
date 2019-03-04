package com.presisco.lazystorm.bolt

import org.slf4j.LoggerFactory

class MapStripBolt(
        private val stripList: ArrayList<String>
) : MapOpBolt() {
    private val logger = LoggerFactory.getLogger(MapRenameBolt::class.java)

    override fun operate(input: Map<String, *>): HashMap<String, *> {
        val output = hashMapOf<String, Any?>()

        input.forEach { key, value ->
            if (key !in stripList) {
                output[key] = value
            }
        }

        return output
    }

}