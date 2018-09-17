package com.presisco.lazystorm.connector

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.util.*
import javax.sql.DataSource

class DataSourceLoader : ConnectorLoader<DataSource, String>() {

    override fun getConnector(): DataSource {
        val props = Properties()
        props.putAll(config)
        return HikariDataSource(HikariConfig(props))
    }
}