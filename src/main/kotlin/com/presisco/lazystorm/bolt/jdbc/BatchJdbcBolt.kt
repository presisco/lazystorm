package com.presisco.lazystorm.bolt.jdbc

import com.presisco.lazystorm.bolt.LazyTickBolt
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.apache.storm.task.OutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.tuple.Tuple
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import javax.sql.DataSource

abstract class BatchJdbcBolt<E> : LazyTickBolt<Any>() {
    private val logger = LoggerFactory.getLogger(BaseJdbcBolt::class.java)

    protected lateinit var outputCollector: OutputCollector

    private lateinit var insertQueue: ArrayBlockingQueue<E>
    private lateinit var onBatchExecute: (batch: List<E>) -> Unit

    @Transient
    protected lateinit var dataSource: DataSource

    protected lateinit var dataSourceConfig: HashMap<String, String>
    protected lateinit var tableName: String
    protected var queryTimeout: Int = 2
    protected var rollbackOnBatchFailure: Boolean = true
    protected var batchSize: Int = 1000
    protected var ack: Boolean = true

    fun setDataSourceConfig(config: HashMap<String, String>): BatchJdbcBolt<E> {
        dataSourceConfig = config
        return this
    }

    fun setTableName(name: String): BatchJdbcBolt<E> {
        tableName = name
        return this
    }

    fun setQueryTimeout(timeout: Int): BatchJdbcBolt<E> {
        queryTimeout = timeout
        return this
    }

    fun setRollbackOnFailure(flag: Boolean): BatchJdbcBolt<E> {
        rollbackOnBatchFailure = flag
        return this
    }

    fun setBatchSize(batch: Int): BatchJdbcBolt<E> {
        batchSize = batch
        return this
    }

    fun setAck(flag: Boolean): BatchJdbcBolt<E> {
        ack = flag
        return this
    }

    override fun setTickIntervalSec(intervalSec: Int): BatchJdbcBolt<E> {
        super.setTickIntervalSec(intervalSec)
        return this
    }

    fun setOnBatchFullCallback(func: (batch: List<E>) -> Unit) {
        onBatchExecute = func
    }

    protected fun add2Batch(data: E) {
        synchronized(insertQueue) {
            insertQueue.put(data)
            insertQueue
            if (insertQueue.remainingCapacity() == 0) {
                onBatchExecute(insertQueue.take(insertQueue.size))
            }
        }
    }

    private fun initializeHikariCP() {
        val props = Properties()
        props.putAll(dataSourceConfig)
        dataSource = HikariDataSource(HikariConfig(props))
    }

    override fun prepare(stormConfig: MutableMap<*, *>, context: TopologyContext, outputCollector: OutputCollector) {
        this.outputCollector = outputCollector
        insertQueue = ArrayBlockingQueue(batchSize)
        initializeHikariCP()
    }

    override fun process(tuple: Tuple) {
        val data = getInput(tuple)

        try {
            when (data) {
                is List<*> -> data.forEach { item -> add2Batch(item as E) }
                else -> add2Batch(data as E)
            }
            outputCollector.ack(tuple)
        } catch (e: Exception) {
            outputCollector.reportError(e)
            outputCollector.fail(tuple)
        }
    }

    override fun onTickTuple(tuple: Tuple) {
        synchronized(insertQueue) {
            if (insertQueue.isNotEmpty()) {
                try {
                    onBatchExecute(insertQueue.take(insertQueue.size))
                } catch (e: Exception) {
                    outputCollector.reportError(e)
                }
            }
        }
    }

}