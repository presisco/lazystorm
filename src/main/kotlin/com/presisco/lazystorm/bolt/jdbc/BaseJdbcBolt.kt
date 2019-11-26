package com.presisco.lazystorm.bolt.jdbc

import com.presisco.lazystorm.bolt.LazyBasicBolt
import com.presisco.lazystorm.connector.DataSourceLoader
import com.presisco.lazystorm.connector.DataSourceManager
import com.presisco.lazystorm.lifecycle.Connectable
import org.apache.storm.task.TopologyContext
import org.slf4j.LoggerFactory
import javax.sql.DataSource

abstract class BaseJdbcBolt<T> : LazyBasicBolt<T>(), Connectable<DataSourceLoader> {
    private val logger = LoggerFactory.getLogger(BaseJdbcBolt::class.java)

    @Transient
    protected lateinit var dataSource: DataSource

    protected lateinit var dataSourceLoader: DataSourceLoader
    protected lateinit var tableName: String
    protected var streamTableMap = hashMapOf<String, String>()
    protected var queryTimeout: Int = 2
    protected var rollbackOnBatchFailure: Boolean = true

    override fun connect(loader: DataSourceLoader) {
        dataSourceLoader = loader
    }

    fun setTableName(name: String): BaseJdbcBolt<T> {
        tableName = name
        return this
    }

    fun setStreamTableMap(map: HashMap<String, String>): BaseJdbcBolt<T> {
        streamTableMap = map
        return this
    }

    protected fun getTable(stream: String) = if (streamTableMap.containsKey(stream)) {
        streamTableMap[stream]!!
    } else {
        tableName
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
        dataSource = DataSourceManager.getConnector(dataSourceLoader)
    }

    override fun prepare(topoConf: MutableMap<String, Any>?, context: TopologyContext?) {
        initializeHikariCP()
    }
}