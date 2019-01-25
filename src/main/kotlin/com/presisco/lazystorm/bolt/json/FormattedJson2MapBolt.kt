package com.presisco.lazystorm.bolt.json

import com.presisco.gsonhelper.FormattedMapHelper
import com.presisco.gsonhelper.SimpleHelper

open class FormattedJson2MapBolt(
        private val formatDef: HashMap<String, ArrayList<String>>
) : JsonParseBolt() {
    override fun initHelper(): SimpleHelper<*> = FormattedMapHelper(formatDef.mapValues { it.value.toSet() })
}