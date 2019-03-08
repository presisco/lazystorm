package com.presisco.lazystorm.bolt.kafka

import com.presisco.lazystorm.bolt.Constants
import com.presisco.lazystorm.bolt.LazyBasicBolt
import org.apache.storm.topology.BasicOutputCollector
import org.apache.storm.topology.FailedException
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.apache.storm.tuple.Values
import org.slf4j.LoggerFactory

abstract class KafkaKeySwitchBolt<K, V>(
        private val kafkaKey2StreamIdMap: HashMap<K, String>
) : LazyBasicBolt<Any>() {
    private val logger = LoggerFactory.getLogger(KafkaKeySwitchBolt::class.java)

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
        kafkaKey2StreamIdMap.values.forEach { id -> declarer.declareStream(id, Fields(Constants.DATA_FIELD_NAME)) }
    }
}