package com.presisco.lazystorm.bolt

import org.apache.storm.topology.BasicOutputCollector
import org.apache.storm.tuple.Tuple

class TupleConsoleDumpBolt : LazyBasicBolt<Any>() {
    override fun execute(tuple: Tuple, outputCollector: BasicOutputCollector) {
        val fields = tuple.fields
        val values = tuple.values
        val reconstructed = hashMapOf<String, Any?>()
        fields.forEachIndexed { index, key -> reconstructed[key] = values[index] }
        println(reconstructed)
    }
}