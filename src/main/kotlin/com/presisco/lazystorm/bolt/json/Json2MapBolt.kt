package com.presisco.lazystorm.bolt.json

import com.presisco.gsonhelper.MapHelper
import org.apache.storm.task.TopologyContext

class Json2MapBolt : JsonParseBolt(){

    @Transient
    private lateinit var mapHelper: MapHelper

    init {
        setParseFunc { json: String -> mapHelper.fromJson(json) }
    }

    override fun prepare(stormConf: MutableMap<Any?, Any?>, context: TopologyContext) {
        mapHelper = MapHelper()
    }
}