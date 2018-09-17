package com.presisco.lazystorm.connector

abstract class ConnectorManager<CONNECTOR> {
    @Transient
    private val connectors = mutableMapOf<String, CONNECTOR>()

    @Synchronized
    fun getConnector(loader: ConnectorLoader<CONNECTOR, *>): CONNECTOR {
        if (connectors.containsKey(loader.name)) {
            connectors[loader.name] = loader.getConnector()
        }
        return connectors[loader.name]!!
    }

}