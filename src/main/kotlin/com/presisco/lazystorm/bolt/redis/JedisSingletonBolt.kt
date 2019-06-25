package com.presisco.lazystorm.bolt.redis

import com.presisco.lazystorm.bolt.LazyBasicBolt
import com.presisco.lazystorm.connector.JedisPoolLoader
import com.presisco.lazystorm.connector.JedisPoolManager
import org.apache.storm.task.TopologyContext
import org.slf4j.LoggerFactory
import redis.clients.jedis.JedisCommands
import redis.clients.jedis.JedisPool
import java.io.Closeable
import java.io.IOException

abstract class JedisSingletonBolt<T> : LazyBasicBolt<T>() {
    private val logger = LoggerFactory.getLogger(JedisSingletonBolt::class.java)

    protected lateinit var jedisPoolLoader: JedisPoolLoader
    protected var keyName: String? = null
    protected var streamKeyMap = hashMapOf<String, String>()

    @Transient
    protected lateinit var jedisPool: JedisPool

    fun setJedisPoolLoader(loader: JedisPoolLoader): JedisSingletonBolt<T> {
        jedisPoolLoader = loader
        return this
    }

    fun setDataKey(key: String): JedisSingletonBolt<T> {
        keyName = key
        return this
    }

    fun setStreamKeyMap(map: HashMap<String, String>): JedisSingletonBolt<T> {
        streamKeyMap = map
        return this
    }

    fun getKey(stream: String) = if (streamKeyMap.containsKey(stream)) {
        streamKeyMap[stream]
    } else {
        keyName
    }

    override fun prepare(topoConf: MutableMap<String, Any>?, context: TopologyContext?) {
        jedisPool = JedisPoolManager.getConnector(jedisPoolLoader)
    }

    fun getCommand() = jedisPool.resource!!

    fun closeCommand(cmd: JedisCommands?) {
        cmd ?: return
        try {
            (cmd as Closeable).close()
        } catch (e: IOException) {
            logger.error("Failed to close (return) instance to pool")
        }
    }
}