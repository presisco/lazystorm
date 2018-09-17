package com.presisco.lazystorm.connector

import redis.clients.jedis.JedisPool

object JedisPoolManager : ConnectorManager<JedisPool>()