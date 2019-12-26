package com.presisco.lazystorm.bolt.kafka

import com.presisco.lazystorm.DATA_FIELD_NAME
import com.presisco.lazystorm.bolt.LazyBasicBolt
import com.presisco.lazystorm.getMap
import com.presisco.lazystorm.getString
import com.presisco.lazystorm.lifecycle.Configurable
import com.presisco.lazystorm.mapKeyToHashMap
import org.apache.storm.topology.BasicOutputCollector
import org.apache.storm.topology.FailedException
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.apache.storm.tuple.Values
import org.slf4j.LoggerFactory

abstract class KafkaKeySwitchBolt<K, V> : LazyBasicBolt<Any>(), Configurable {
    private val logger = LoggerFactory.getLogger(KafkaKeySwitchBolt::class.java)

    private lateinit var kafkaKey2StreamIdMap: HashMap<K, String>

    override fun configure(config: Map<String, *>) {
        with(config) {
            val keyType = getString("key_type")
            val valueType = getString("value_type")
            if (valueType !in setOf("string")) {
                throw IllegalStateException("unsupported value type: $valueType")
            }
            val keyStreamMap = getMap<String, String>("key_stream_map")
            val converted = when (keyType) {
                "int" -> keyStreamMap.mapKeyToHashMap { Integer.parseInt(it) }
                "short" -> keyStreamMap.mapKeyToHashMap { Integer.parseInt(it).toShort() }
                "string" -> keyStreamMap.mapKeyToHashMap { it }
                else -> throw IllegalStateException("unsupported key type: $keyType")
            }
            kafkaKey2StreamIdMap = converted as HashMap<K, String>
        }
    }

    override fun execute(tuple: Tuple, outputCollector: BasicOutputCollector) {
        try {
            val streamId = kafkaKey2StreamIdMap[tuple.getValueByField("key") as K]
            val data = tuple.getValueByField("value") as V
            outputCollector.emit(streamId, Values(data))
        } catch (e: Exception) {
            throw FailedException("failed to send data", e)
        }
    }

    override fun declareOutputFields(declarer: OutputFieldsDeclarer) {
        kafkaKey2StreamIdMap.values.forEach { id -> declarer.declareStream(id, Fields(DATA_FIELD_NAME)) }
    }
}