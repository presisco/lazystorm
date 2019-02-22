package com.presisco.lazystorm.bolt.kafka

import com.presisco.gsonhelper.MapHelper
import org.apache.storm.kafka.bolt.mapper.TupleToKafkaMapper
import org.apache.storm.tuple.Tuple
import org.slf4j.LoggerFactory

class LazyJsonMapper : TupleToKafkaMapper<String, String> {
    private val logger = LoggerFactory.getLogger(KafkaKeySwitchBolt::class.java)

    override fun getMessageFromTuple(tuple: Tuple): String {
        val mapHelper = MapHelper()
        val keys = tuple.fields
        val map = mutableMapOf<String, Any?>()
        keys.forEach { map[it] = tuple.getValueByField(it) }
        try {
            return mapHelper.toJson(map)
        } catch (e: Exception) {
            logger.error("source: ${tuple.sourceComponent}@${tuple.sourceTask}:${tuple.sourceStreamId}")
            throw e
        }
    }

    override fun getKeyFromTuple(tuple: Tuple) = "${tuple.sourceComponent}@${tuple.sourceTask}:${tuple.sourceStreamId}"
}