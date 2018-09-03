package com.presisco.lazystorm.bolt.jdbc

import com.presisco.lazystorm.bolt.LazyBasicBolt
import com.presisco.lazystorm.datasouce.DataSourceHolder
import org.apache.storm.task.TopologyContext
import org.slf4j.LoggerFactory
import javax.sql.DataSource

abstract class BaseJdbcBolt<T> : LazyBasicBolt<T>() {
    private val logger = LoggerFactory.getLogger(BatchJdbcBolt::class.java)

    @Transient
    protected lateinit var dataSource: DataSource

    protected lateinit var dataSourceHolder: DataSourceHolder
    protected lateinit var tableName: String
    protected var queryTimeout: Int = 2
    protected var rollbackOnBatchFailure: Boolean = true

    fun setDataSource(holder: DataSourceHolder): BaseJdbcBolt<T> {
        dataSourceHolder = holder
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
        dataSource = dataSourceHolder.getDataSource()
    }

    override fun prepare(stormConf: MutableMap<Any?, Any?>, context: TopologyContext) {
        initializeHikariCP()
    }
}