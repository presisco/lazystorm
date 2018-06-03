package com.presisco.lazystorm.bolt.jdbc

import com.presisco.lazystorm.bolt.Constants
import com.presisco.lazystorm.bolt.LazyTickBolt
import jdk.nashorn.internal.runtime.ECMAErrors
import org.apache.storm.Config
import org.apache.storm.task.OutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.base.BaseTickTupleAwareRichBolt
import org.apache.storm.tuple.Tuple
import java.sql.SQLException
import java.util.concurrent.ArrayBlockingQueue
import javax.sql.DataSource

abstract class BatchJdbcBolt<E>(
        srcPos: Int = Constants.DATA_FIELD_POS,
        srcField: String = Constants.DATA_FIELD_NAME,
        protected val dataSource: DataSource,
        protected val tableName: String,
        protected val batchSize: Int = 1000,
        protected val queryTimeout: Int = 2,
        protected val rollbackOnBatchFailure: Boolean = true,
        protected val ack: Boolean = true,
        tickIntervalSec: Int = 5
) : LazyTickBolt<Any>(srcPos, srcField, tickIntervalSec) {
    protected lateinit var outputCollector: OutputCollector

    private lateinit var insertQueue: ArrayBlockingQueue<E>
    private lateinit var onBatchExecute: (batch: List<E>) -> Unit

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

    override fun prepare(stormConfig: MutableMap<*, *>, context: TopologyContext, outputCollector: OutputCollector) {
        this.outputCollector = outputCollector
        insertQueue = ArrayBlockingQueue(batchSize)
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