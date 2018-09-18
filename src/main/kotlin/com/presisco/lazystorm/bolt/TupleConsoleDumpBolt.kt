package com.presisco.lazystorm.bolt

import org.apache.storm.topology.BasicOutputCollector
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseBasicBolt
import org.apache.storm.tuple.Tuple

class TupleConsoleDumpBolt : BaseBasicBolt() {
    override fun execute(tuple: Tuple, outputCollector: BasicOutputCollector) {
        val fields = tuple.fields
        val values = tuple.values
        val reconstructed = hashMapOf<String, Any>()
        fields.forEachIndexed { index, key -> reconstructed[key] = values[index] }
        println(reconstructed.toString())
    }

    override fun declareOutputFields(p0: OutputFieldsDeclarer) {

    }
}