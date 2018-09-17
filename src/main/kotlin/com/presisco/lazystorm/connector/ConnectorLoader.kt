package com.presisco.lazystorm.connector

import java.util.*

abstract class ConnectorLoader<CONNECTOR, CONFIG_VALUE> {
    lateinit var name: String
    lateinit var config: HashMap<String, CONFIG_VALUE>

    fun setConfig(name: String, config: HashMap<String, CONFIG_VALUE>): ConnectorLoader<CONNECTOR, CONFIG_VALUE> {
        this.name = name
        this.config = config
        return this
    }

    abstract fun getConnector(): CONNECTOR
}