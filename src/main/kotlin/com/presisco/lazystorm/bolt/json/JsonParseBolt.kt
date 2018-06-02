package com.presisco.lazystorm.bolt.json

import org.apache.storm.topology.BasicOutputCollector
import org.apache.storm.topology.FailedException
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseBasicBolt
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.apache.storm.tuple.Values
import org.slf4j.LoggerFactory

open class JsonParseBolt: BaseBasicBolt() {
    private val logger = LoggerFactory.getLogger(JsonParseBolt::class.java)

    private var srcPos: Int = 0
    private var srcField: String = ""

    private lateinit var parse: (json: String) -> Any

    fun setJsonPosition(pos: Int){
        srcPos = pos
    }

    fun setJsonField(field: String){
        srcField = field
    }

    fun setParseFunc(func: (json: String) -> Any){
        parse = func
    }

    override fun execute(tuple: Tuple, outputCollector: BasicOutputCollector) {

        val json = if (srcField != "")
            tuple.getStringByField(srcField)
        else if (srcPos >= 0)
            tuple.getString(srcPos)
        else {
            logger.debug("invalid src params! try getting from pos: 0")
            tuple.getString(0)
        }

        try {
            val parsed = parse(json)
            outputCollector.emit(Values("data", parsed))
        } catch (e: Exception) {
            logger.warn("parse exception: ${e.message}")
            logger.warn("raw data: $json")
            throw FailedException("parse exception: ${e.message}")
        }
    }

    override fun declareOutputFields(declarer: OutputFieldsDeclarer) = declarer.declare(Fields("data"))


}