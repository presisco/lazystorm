package com.presisco.lazystorm.bolt.kafka

import com.presisco.gsonhelper.MapHelper
import org.apache.storm.kafka.bolt.mapper.TupleToKafkaMapper
import org.apache.storm.tuple.Tuple

class LazyJsonMapper : TupleToKafkaMapper<String, String> {

    @Transient
    private val mapHelper = MapHelper()

    override fun getMessageFromTuple(tuple: Tuple): String {
        val keys = tuple.fields
        val map = mutableMapOf<String, Any?>()
        keys.forEach { map[it] = tuple.getValueByField(it) }
        return mapHelper.toJson(map)
    }

    override fun getKeyFromTuple(tuple: Tuple) = "${tuple.sourceComponent}@${tuple.sourceTask}:${tuple.sourceStreamId}"
}