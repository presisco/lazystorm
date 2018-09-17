package com.presisco.lazystorm.connector

import redis.clients.jedis.JedisCluster

object JedisClusterManager : ConnectorManager<JedisCluster>()