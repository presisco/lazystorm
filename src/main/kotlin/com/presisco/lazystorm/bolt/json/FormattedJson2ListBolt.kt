package com.presisco.lazystorm.bolt.json

import com.presisco.gsonhelper.FormattedListHelper
import com.presisco.gsonhelper.SimpleHelper

open class FormattedJson2ListBolt(
        private val formatDef: HashMap<String, ArrayList<String>>
) : JsonParseBolt() {
    override fun initHelper(): SimpleHelper<*> = FormattedListHelper(formatDef.mapValues { it.value.toSet() })
}