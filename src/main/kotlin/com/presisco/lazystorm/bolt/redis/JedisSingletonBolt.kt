package com.presisco.lazystorm.bolt.redis

import com.presisco.lazystorm.bolt.LazyBasicBolt
import com.presisco.lazystorm.connector.JedisPoolLoader
import com.presisco.lazystorm.connector.JedisPoolManager
import com.presisco.lazystorm.getHashMap
import com.presisco.lazystorm.getString
import com.presisco.lazystorm.lifecycle.Configurable
import com.presisco.lazystorm.lifecycle.Connectable
import org.apache.storm.task.TopologyContext
import org.slf4j.LoggerFactory
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import java.io.Closeable
import java.io.IOException

abstract class JedisSingletonBolt<T> : LazyBasicBolt<T>(), Connectable<JedisPoolLoader>, Configurable {
    private val logger = LoggerFactory.getLogger(JedisSingletonBolt::class.java)

    protected lateinit var jedisPoolLoader: JedisPoolLoader
    protected var keyName: String? = null
    protected var streamKeyMap = hashMapOf<String, String>()

    @Transient
    protected lateinit var jedisPool: JedisPool

    override fun connect(loader: JedisPoolLoader) {
        this.jedisPoolLoader = loader
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

    override fun configure(config: Map<String, *>) {
        with(config) {
            if (containsKey("stream_key_map")) {
                setStreamKeyMap(getHashMap("stream_key_map") as java.util.HashMap<String, String>)
            }
            if (containsKey("key")) {
                setDataKey(getString("key"))
            }
        }
    }

    override fun prepare(stormConf: MutableMap<Any?, Any?>?, context: TopologyContext?) {
        jedisPool = JedisPoolManager.getConnector(jedisPoolLoader)
    }

    fun getCommand() = jedisPool.resource!!

    fun closeCommand(cmd: Jedis?) {
        cmd ?: return
        try {
            (cmd as Closeable).close()
        } catch (e: IOException) {
            logger.error("Failed to close (return) instance to pool")
        }
    }
}