package com.presisco.lazystorm.bolt.redis

import com.presisco.gsonhelper.FuzzyHelper
import com.presisco.lazystorm.mapValueToHashMap
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.BasicOutputCollector
import org.apache.storm.tuple.Tuple

class JedisMapToHashBolt : JedisSingletonBolt<Map<String, *>>() {
    @Transient
    lateinit var fuzzyHelper: FuzzyHelper

    override fun prepare(stormConf: MutableMap<Any?, Any?>?, context: TopologyContext?) {
        super.prepare(stormConf, context)
        fuzzyHelper = FuzzyHelper()
    }

    override fun execute(tuple: Tuple, outputCollector: BasicOutputCollector) {
        val jedisCmd = getCommand()
        jedisCmd.hmset(getKey(tuple.sourceStreamId), getInput(tuple).mapValueToHashMap { value ->
            when (value) {
                is String -> value
                is Map<*, *> -> fuzzyHelper.toJson(value)
                is List<*> -> fuzzyHelper.toJson(value)
                else -> value.toString()
            }
        })
        closeCommand(jedisCmd)
    }

}