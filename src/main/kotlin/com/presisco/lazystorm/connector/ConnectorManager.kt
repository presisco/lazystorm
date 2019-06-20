package com.presisco.lazystorm.connector

import org.apache.log4j.Logger

abstract class ConnectorManager<CONNECTOR> {

    @Transient
    private val connectors = mutableMapOf<String, CONNECTOR>()

    @Synchronized
    fun getConnector(loader: ConnectorLoader<CONNECTOR>): CONNECTOR {
        val logger = Logger.getLogger(ConnectorManager::class.java)
        if (!connectors.containsKey(loader.name)) {
            logger.info("creating connector for ${loader.name}")
            connectors[loader.name] = loader.getConnector()
        }
        return connectors[loader.name]!!
    }

}