package com.presisco.lazystorm.bolt.json

import com.presisco.gsonhelper.SimpleHelper
import com.presisco.lazystorm.bolt.LazyBasicBolt
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.BasicOutputCollector
import org.apache.storm.topology.FailedException
import org.apache.storm.tuple.Tuple
import org.apache.storm.tuple.Values
import org.slf4j.LoggerFactory

abstract class JsonParseBolt : LazyBasicBolt<String>() {
    private val logger = LoggerFactory.getLogger(JsonParseBolt::class.java)

    @Transient
    private lateinit var jsonHelper: SimpleHelper<*>

    abstract fun initHelper(): SimpleHelper<*>

    override fun prepare(stormConf: MutableMap<Any?, Any?>?, context: TopologyContext?) {
        super.prepare(stormConf, context)
        jsonHelper = initHelper()
    }

    override fun execute(tuple: Tuple, outputCollector: BasicOutputCollector) {
        val json = getInput(tuple)

        try {
            val parsed = jsonHelper.fromJson(json)
            outputCollector.emitData(Values(parsed))
        } catch (e: Exception) {
            logger.warn("parse exception: ${e.message}")
            logger.warn("raw data: $json")
            throw FailedException("parse exception: ${e.message}")
        }
    }

}