package com.presisco.lazystorm.connector

import redis.clients.jedis.HostAndPort
import redis.clients.jedis.JedisCluster
import redis.clients.jedis.JedisPoolConfig

class JedisClusterLoader : ConnectorLoader<JedisCluster>() {
    override fun getConnector(): JedisCluster {
        val hosts = config["hosts"]!!.split(",")
        val hostSet = mutableSetOf<HostAndPort>()
        hosts.forEach { hostSet.add(HostAndPort(it.substringBefore(":"), it.substringAfter(":").toInt())) }
        val poolConfig = JedisPoolConfig()
        if (config.containsKey("max_total")) {
            poolConfig.maxTotal = config["max_total"]!!.toInt()
        }
        if (config.containsKey("max_idle")) {
            poolConfig.maxIdle = config["max_idle"]!!.toInt()
        }
        if (config.containsKey("min_idle")) {
            poolConfig.minIdle = config["min_idle"]!!.toInt()
        }
        if (config.containsKey("max_wait_millis")) {
            poolConfig.maxWaitMillis = config["max_wait_millis"]!!.toLong()
        }
        return JedisCluster(
                hostSet,
                poolConfig
        )
    }
}