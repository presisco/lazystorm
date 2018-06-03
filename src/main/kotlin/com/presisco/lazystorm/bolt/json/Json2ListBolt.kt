package com.presisco.lazystorm.bolt.json

import com.presisco.gsonhelper.ListHelper

class Json2ListBolt : JsonParseBolt() {

    @Transient
    private val listHelper = ListHelper()

    init {
        setParseFunc { json: String -> listHelper.fromJson(json) }
    }
}