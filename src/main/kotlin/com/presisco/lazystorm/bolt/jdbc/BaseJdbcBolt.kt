package com.presisco.lazystorm.bolt.jdbc

import com.presisco.lazystorm.bolt.LazyBasicBolt
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.apache.storm.task.TopologyContext
import org.slf4j.LoggerFactory
import java.util.*
import javax.sql.DataSource

abstract class BaseJdbcBolt<T> : LazyBasicBolt<T>() {
    private val logger = LoggerFactory.getLogger(BatchJdbcBolt::class.java)

    @Transient
    protected lateinit var dataSource: DataSource

    protected lateinit var dataSourceConfig: HashMap<String, String>
    protected lateinit var tableName: String
    protected var queryTimeout: Int = 2
    protected var rollbackOnBatchFailure: Boolean = true

    fun setDataSourceConfig(config: HashMap<String, String>): BaseJdbcBolt<T> {
        dataSourceConfig = config
        return this
    }

    fun setTableName(name: String): BaseJdbcBolt<T> {
        tableName = name
        return this
    }

    fun setQueryTimeout(timeout: Int): BaseJdbcBolt<T> {
        queryTimeout = timeout
        return this
    }

    fun setRollbackOnFailure(flag: Boolean): BaseJdbcBolt<T> {
        rollbackOnBatchFailure = flag
        return this
    }

    private fun initializeHikariCP() {
        val props = Properties()
        props.putAll(dataSourceConfig)
        dataSource = HikariDataSource(HikariConfig(props))
    }

    override fun prepare(stormConf: MutableMap<Any?, Any?>, context: TopologyContext) {
        initializeHikariCP()
    }

    companion object
}