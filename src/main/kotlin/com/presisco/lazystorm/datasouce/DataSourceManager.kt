package com.presisco.lazystorm.datasouce

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.util.*
import javax.sql.DataSource

object DataSourceManager {

    private val dataSourceConfigs = HashMap<String, HashMap<String, String>>()
    @Transient
    private val dataSources = mutableMapOf<String, DataSource>()

    fun loadDataSourceConfig(conf: Map<String, Map<String, String>>) {
        conf.forEach { name, config ->
            dataSourceConfigs[name] = HashMap()
            config.forEach { key, value ->
                dataSourceConfigs[name]!![key] = value
            }
        }
    }

    @Synchronized
    fun getDataSource(name: String?): DataSource {
        name ?: throw NullPointerException("DataSource name is null!")
        if (!dataSources.containsKey(name)) {
            val props = Properties()
            props.putAll(dataSourceConfigs[name]!!)
            dataSources[name] = HikariDataSource(HikariConfig(props))
        }
        return dataSources[name]!!
    }

}