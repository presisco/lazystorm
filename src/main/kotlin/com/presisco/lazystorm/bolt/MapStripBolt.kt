package com.presisco.lazystorm.bolt

import com.presisco.lazystorm.getArrayList
import com.presisco.lazystorm.lifecycle.Configurable
import org.slf4j.LoggerFactory

class MapStripBolt : MapOpBolt(), Configurable {
    private val logger = LoggerFactory.getLogger(MapStripBolt::class.java)
    private lateinit var stripList: ArrayList<String>

    override fun configure(config: Map<String, *>) {
        stripList = config.getArrayList("strip")
    }

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