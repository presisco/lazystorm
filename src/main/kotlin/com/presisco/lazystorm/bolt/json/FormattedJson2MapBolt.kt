package com.presisco.lazystorm.bolt.json

import com.presisco.gsonhelper.FormattedMapHelper
import com.presisco.gsonhelper.SimpleHelper
import com.presisco.lazystorm.collectionToArrayList
import com.presisco.lazystorm.lifecycle.Configurable
import com.presisco.lazystorm.mapValueToHashMap

open class FormattedJson2MapBolt : JsonParseBolt(), Configurable {

    private lateinit var formatDef: HashMap<String, ArrayList<String>>

    override fun configure(config: Map<String, *>) {
        val formatDefRaw = config["format"] as Map<String, Collection<String>>
        formatDef = formatDefRaw.mapValueToHashMap { collectionToArrayList(it) }
    }

    override fun initHelper(): SimpleHelper<*> = FormattedMapHelper(formatDef.mapValues { it.value.toSet() })
}