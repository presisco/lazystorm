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

abstract class BaseJedisPoolBolt<T> : LazyBasicBolt<T>() {
    private val logger = LoggerFactory.getLogger(BaseJedisPoolBolt::class.java)

    @Transient
    protected lateinit var jedisPool: JedisPool
    protected lateinit var jedisPoolLoader: JedisPoolLoader
    protected lateinit var keyName: String

    fun setJedisPoolLoader(loader: JedisPoolLoader): BaseJedisPoolBolt<T> {
        jedisPoolLoader = loader
        return this
    }

    fun setDataKey(key: String): BaseJedisPoolBolt<T> {
        keyName = key
        return this
    }

    fun initializeJedisPool() {
        jedisPool = JedisPoolManager.getConnector(jedisPoolLoader)
    }

    override fun prepare(stormConf: MutableMap<*, *>, context: TopologyContext) {
        initializeJedisPool()
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