package com.presisco.lazystorm.connector

import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig

class JedisPoolLoader : ConnectorLoader<JedisPool>() {
    override fun getConnector(): JedisPool {
        val defaultConfig = JedisPoolConfig()
        return JedisPool(
                defaultConfig,
                config["host"],
                config["port"]!!.toInt(),
                config["timeout"]!!.toInt(),
                config["password"],
                config["database"]!!.toInt()
        )
    }
}