package com.presisco.lazystorm.bolt.json

import com.presisco.gsonhelper.MapHelper

class Json2MapBolt : JsonParseBolt(){
    override fun initHelper() = MapHelper()
}