package com.presisco.lazystorm.bolt

import com.presisco.lazystorm.*
import org.apache.storm.task.OutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseWindowedBolt
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.apache.storm.tuple.Values
import org.apache.storm.windowing.TupleWindow
import org.slf4j.LoggerFactory

abstract class LazyWindowedBolt<T>(
        private var srcPos: Int = DATA_FIELD_POS,
        private var srcField: String = DATA_FIELD_NAME
) : BaseWindowedBolt() {
    private val logger = LoggerFactory.getLogger(LazyWindowedBolt::class.java)

    var customDataStreams = ArrayList<String>()

    fun setSrcPos(pos: Int): LazyWindowedBolt<T> {
        srcPos = pos
        return this
    }

    fun setSrcField(field: String): LazyWindowedBolt<T> {
        srcField = field
        return this
    }

    fun getInput(tuple: Tuple) = if (srcPos != DATA_FIELD_POS)
        tuple.getValue(srcPos) as T
    else
        tuple.getValueByField(srcField) as T

    protected fun TupleWindow.toDataList(): List<T> = this.get().map { getInput(it) }

    protected fun Tuple.toDataMap(): Map<String, *> = this.getValueByField(DATA_FIELD_NAME) as Map<String, *>

    fun getArrayListInput(tuple: Tuple): ArrayList<out T> {
        val fuzzy = getInput(tuple)

        return if (fuzzy !is List<*>) {
            arrayListOf(fuzzy)
        } else {
            fuzzy as ArrayList<T>
        }
    }

    protected fun emitData(data: Any) = collector.emit(DATA_STREAM_NAME, Values(data))

    protected fun emitFailed(data: Any, msg: String, time: String) = collector.emit(FAILED_STREAM_NAME, Values(data, msg, time))

    protected fun emitStats(data: Any, time: String) = collector.emit(STATS_STREAM_NAME, Values(data, time))

    protected fun emitDataToStreams(sourceStream: String, data: Any) = if (sourceStream in customDataStreams) {
        collector.emit(sourceStream, Values(data))
    } else {
        emitData(data)
    }

    @Transient
    protected lateinit var collector: OutputCollector

    override fun prepare(topoConf: Map<String, *>, context: TopologyContext, collector: OutputCollector) {
        super.prepare(topoConf, context, collector)
        this.collector = collector
    }

    override fun declareOutputFields(declarer: OutputFieldsDeclarer) {
        declarer.declareStream(
                DATA_STREAM_NAME,
                Fields(DATA_FIELD_NAME))
        declarer.declareStream(
                STATS_STREAM_NAME,
                Fields(
                        DATA_FIELD_NAME,
                        STATS_TIME
                )
        )
        declarer.declareStream(
                FAILED_STREAM_NAME,
                Fields(
                        DATA_FIELD_NAME,
                        FAILED_MESSAGE_FIELD,
                        FAILED_TIME
                )
        )
        customDataStreams.filter {
            it !in setOf(
                    DATA_STREAM_NAME,
                    STATS_STREAM_NAME,
                    FAILED_STREAM_NAME
            )
        }.forEach {
            declarer.declareStream(it, Fields(DATA_FIELD_NAME))
        }
    }
}