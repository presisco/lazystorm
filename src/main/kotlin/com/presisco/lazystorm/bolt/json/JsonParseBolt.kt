package com.presisco.lazystorm.bolt.json

import com.presisco.lazystorm.bolt.LazyBasicBolt
import org.apache.storm.topology.BasicOutputCollector
import org.apache.storm.topology.FailedException
import org.apache.storm.tuple.Tuple
import org.apache.storm.tuple.Values
import org.slf4j.LoggerFactory

open class JsonParseBolt() : LazyBasicBolt<String>() {
    private val logger = LoggerFactory.getLogger(JsonParseBolt::class.java)

    private lateinit var parse: (json: String) -> Any

    constructor(func: (json: String) -> Any) : this() {
        parse = func
    }

    fun setParseFunc(func: (json: String) -> Any){
        parse = func
    }

    override fun execute(tuple: Tuple, outputCollector: BasicOutputCollector) {

        val json = getInput(tuple)

        try {
            val parsed = parse(json)
            outputCollector.emit(Values(parsed))
        } catch (e: Exception) {
            logger.warn("parse exception: ${e.message}")
            logger.warn("raw data: $json")
            throw FailedException("parse exception: ${e.message}")
        }
    }

}