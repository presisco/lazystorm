package com.presisco.lazystorm.connector

import java.io.Serializable
import java.util.*

abstract class ConnectorLoader<CONNECTOR> : Serializable {
    lateinit var name: String
    lateinit var config: HashMap<String, String>

    fun setConfig(name: String, config: HashMap<String, String>): ConnectorLoader<CONNECTOR> {
        this.name = name
        this.config = config
        return this
    }

    abstract fun getConnector(): CONNECTOR
}