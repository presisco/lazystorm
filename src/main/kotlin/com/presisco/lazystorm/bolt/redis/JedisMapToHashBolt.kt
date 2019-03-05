package com.presisco.lazystorm.bolt.redis

import org.apache.storm.topology.BasicOutputCollector
import org.apache.storm.tuple.Tuple

class JedisMapToHashBolt : JedisSingletonBolt<HashMap<String, Any?>>() {

    override fun execute(tuple: Tuple, outputCollector: BasicOutputCollector) {
        val hashMap = hashMapOf<String, String>()
        tuple.fields.forEachIndexed { index, field ->
            hashMap[field] = tuple.getValue(index).toString()
        }
        val jedisCmd = getCommand()
        jedisCmd.hmset(getKey(tuple.sourceStreamId), hashMap)
        closeCommand(jedisCmd)
    }

}