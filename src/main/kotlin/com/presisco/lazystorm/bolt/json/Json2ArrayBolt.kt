package com.presisco.lazystorm.bolt.json

import com.presisco.gsonhelper.ArrayHelper

class Json2ArrayBolt : JsonParseBolt() {

    @Transient
    private val arrayHelper = ArrayHelper()

    init {
        setParseFunc { json: String -> arrayHelper.fromJson(json) }
    }
}