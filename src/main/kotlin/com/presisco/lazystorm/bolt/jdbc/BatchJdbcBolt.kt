package com.presisco.lazystorm.bolt.jdbc

import com.presisco.lazystorm.bolt.LazyTickBolt
import com.presisco.lazystorm.connector.DataSourceLoader
import org.apache.storm.task.OutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.tuple.Tuple
import org.slf4j.LoggerFactory
import java.util.concurrent.ArrayBlockingQueue
import javax.sql.DataSource

abstract class BatchJdbcBolt<E> : LazyTickBolt<Any>() {
    private val logger = LoggerFactory.getLogger(BaseJdbcBolt::class.java)

    protected lateinit var outputCollector: OutputCollector

    private lateinit var dataQueue: ArrayBlockingQueue<E>
    private lateinit var onBatchExecute: (batch: List<E>) -> Unit

    @Transient
    protected lateinit var dataSource: DataSource

    protected lateinit var dataSourceLoader: DataSourceLoader
    protected lateinit var tableName: String
    protected var queryTimeout: Int = 2
    protected var rollbackOnBatchFailure: Boolean = true
    protected var batchSize: Int = 1000
    protected var ack: Boolean = true

    fun setDataSource(loader: DataSourceLoader): BatchJdbcBolt<E> {
        dataSourceLoader = loader
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
        synchronized(dataQueue) {
            dataQueue.put(data)
            dataQueue
            if (dataQueue.remainingCapacity() == 0) {
                logger.info("queue full! execute batch")
                onBatchExecute(dataQueue.take(dataQueue.size))
            }
        }
    }

    private fun initializeHikariCP() {
        dataSource = dataSourceLoader.getConnector()
    }

    override fun prepare(stormConfig: MutableMap<String, Any>, context: TopologyContext, outputCollector: OutputCollector) {
        this.outputCollector = outputCollector
        dataQueue = ArrayBlockingQueue(batchSize)
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
        synchronized(dataQueue) {
            if (dataQueue.isNotEmpty()) {
                try {
                    onBatchExecute(dataQueue.take(dataQueue.size))
                } catch (e: Exception) {
                    outputCollector.reportError(e)
                }
            }
        }
    }

}