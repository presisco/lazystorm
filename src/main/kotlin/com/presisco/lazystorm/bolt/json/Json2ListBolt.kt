package com.presisco.lazystorm.bolt.json

import com.presisco.gsonhelper.ListHelper

class Json2ListBolt : JsonParseBolt() {
    override fun initHelper() = ListHelper()
}