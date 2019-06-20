package com.presisco.lazystorm.connector

object LoaderManager {
    private val managerMap = hashMapOf<String, HashMap<String, ConnectorLoader<*>>>()

    fun addType(type: String, configs: Map<String, HashMap<String, String>>) {
        val constructor = when (type) {
            "data_source" -> ::DataSourceLoader
            "redis" -> ::JedisPoolLoader
            "neo4j" -> ::Neo4jLoader
            else -> throw IllegalStateException("unknown connector type: $type")
        }
        managerMap[type] = hashMapOf()
        configs.forEach { name, config -> managerMap[type]!![name] = constructor().setConfig(name, config) }
    }

    fun <LOADER> getLoader(type: String, name: String) = if (managerMap.containsKey(type)) {
        if (managerMap[type]!!.containsKey(name)) {
            managerMap[type]!![name]!! as LOADER
        } else {
            throw IllegalStateException("unknown connector name: $name")
        }
    } else {
        throw IllegalStateException("unknown connector type: $type")
    }

}