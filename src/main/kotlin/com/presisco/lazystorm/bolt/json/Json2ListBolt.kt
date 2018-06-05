package com.presisco.lazystorm.bolt.json

import com.presisco.gsonhelper.ListHelper
import org.apache.storm.task.TopologyContext

class Json2ListBolt : JsonParseBolt() {

    @Transient
    private lateinit var listHelper: ListHelper

    init {
        setParseFunc { json: String -> listHelper.fromJson(json) }
    }

    override fun prepare(stormConf: MutableMap<Any?, Any?>?, context: TopologyContext?) {
        listHelper = ListHelper()
    }
}