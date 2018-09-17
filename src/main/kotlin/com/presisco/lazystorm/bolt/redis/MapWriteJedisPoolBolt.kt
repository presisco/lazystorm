package com.presisco.lazystorm.bolt.redis

import org.apache.storm.topology.BasicOutputCollector
import org.apache.storm.topology.FailedException
import org.apache.storm.tuple.Tuple

class MapWriteJedisPoolBolt : BaseJedisPoolBolt<Any>() {

    fun writeMap(dataMap: HashMap<String, *>) {
        val jedisCmd = getCommand()
        val converted = HashMap<String, String>()
        dataMap.forEach { key, value -> converted[key] = value.toString() }
        jedisCmd.hmset(keyName, converted)
        closeCommand(jedisCmd)
    }

    fun writeList(dataMapList: List<Map<String, *>>) {

    }

    override fun execute(tuple: Tuple, outputCollector: BasicOutputCollector) {
        val data = getInput(tuple)
        when (data) {
            is HashMap<*, *> -> writeMap(data as HashMap<String, *>)
            is List<*> -> writeList(data as List<Map<String, *>>)
            else -> throw FailedException("unsupported data type: ${data::class.java.simpleName}")
        }
    }
}