package com.presisco.lazystorm.bolt.json

import com.presisco.gsonhelper.FormattedListHelper
import org.apache.storm.task.TopologyContext

open class FormattedJson2ListBolt(
        private val formatDef: Map<String, Set<String>>
) : JsonParseBolt() {

    @Transient
    private lateinit var formattedListHelper: FormattedListHelper

    init {
        setParseFunc { formattedListHelper.fromJson(it) }
    }

    override fun prepare(stormConf: MutableMap<Any?, Any?>?, context: TopologyContext?) {
        formattedListHelper = FormattedListHelper(formatDef)
    }
}