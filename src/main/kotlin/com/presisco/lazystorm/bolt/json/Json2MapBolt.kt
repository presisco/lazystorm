package com.presisco.lazystorm.bolt.json

import com.presisco.gsonhelper.MapHelper

class Json2MapBolt : JsonParseBolt(){

    @Transient
    private val mapHelper = MapHelper()

    init {
        setParseFunc { json: String -> mapHelper.fromJson(json) }
    }
}