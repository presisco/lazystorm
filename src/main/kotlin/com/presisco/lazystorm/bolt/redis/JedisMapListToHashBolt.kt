package com.presisco.lazystorm.bolt.redis

import com.presisco.gsonhelper.MapHelper
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.BasicOutputCollector
import org.apache.storm.tuple.Tuple
import org.slf4j.LoggerFactory

class JedisMapListToHashBolt(private val keyField: String) : JedisSingletonBolt<HashMap<String, Any?>>() {
    private val logger = LoggerFactory.getLogger(JedisMapListToHashBolt::class.java)

    @Transient
    private lateinit var mapHelper: MapHelper

    override fun prepare(topoConf: Map<String, *>, context: TopologyContext) {
        super.prepare(topoConf, context)
        mapHelper = MapHelper()
    }

    fun writeDataSet(key: String, dataMap: HashMap<String, String>) {
        val jedisCmd = getCommand()
        jedisCmd.hmset(key, dataMap)
        closeCommand(jedisCmd)
    }

    override fun execute(tuple: Tuple, outputCollector: BasicOutputCollector) {
        val dataSet = getArrayListInput(tuple)
        val dataMap = hashMapOf<String, String>()
        dataSet.forEach {
            val json = mapHelper.toJson(it)
            dataMap[it[keyField].toString()] = json
        }
        writeDataSet(getKey(tuple.sourceStreamId)!!, dataMap)
    }
}