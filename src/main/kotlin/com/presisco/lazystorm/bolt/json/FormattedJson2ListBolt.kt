package com.presisco.lazystorm.bolt.json

import com.presisco.gsonhelper.FormattedListHelper
import com.presisco.gsonhelper.SimpleHelper
import com.presisco.lazystorm.collectionToArrayList
import com.presisco.lazystorm.lifecycle.Configurable
import com.presisco.lazystorm.mapValueToHashMap

open class FormattedJson2ListBolt : JsonParseBolt(), Configurable {
    private lateinit var formatDef: HashMap<String, ArrayList<String>>

    override fun configure(config: Map<String, *>) {
        val formatDefRaw = config["format"] as Map<String, Collection<String>>
        formatDef = formatDefRaw.mapValueToHashMap { collectionToArrayList(it) }
    }

    override fun initHelper(): SimpleHelper<*> = FormattedListHelper(formatDef.mapValues { it.value.toSet() })
}