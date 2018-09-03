package com.presisco.lazystorm.datasouce

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.io.Serializable
import java.util.*
import javax.sql.DataSource

class DataSourceHolder(
        val name: String,
        val config: HashMap<String, String>
) : Serializable {

    fun getDataSource() = DataSourceHolder.getDataSource(name, config)

    companion object {
        @Transient
        private val dataSources = mutableMapOf<String, DataSource>()

        @Synchronized
        fun getDataSource(name: String, config: HashMap<String, String>): DataSource {
            if (!dataSources.containsKey(name)) {
                val props = Properties()
                props.putAll(config)
                dataSources[name] = HikariDataSource(HikariConfig(props))
            }
            return dataSources[name]!!
        }
    }
}