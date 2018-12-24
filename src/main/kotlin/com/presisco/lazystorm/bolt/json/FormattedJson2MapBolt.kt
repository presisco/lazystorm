package com.presisco.lazystorm.bolt.json

import com.presisco.gsonhelper.FormattedMapHelper
import org.apache.storm.task.TopologyContext

open class FormattedJson2MapBolt(
        private val formatDef: Map<String, Set<String>>
) : JsonParseBolt() {

    @Transient
    private lateinit var formattedMapHelper: FormattedMapHelper

    init {
        setParseFunc { formattedMapHelper.fromJson(it) }
    }

    override fun prepare(stormConf: MutableMap<Any?, Any?>?, context: TopologyContext?) {
        formattedMapHelper = FormattedMapHelper(formatDef)
    }
}