package com.presisco.lazystorm.bolt

import com.presisco.lazystorm.*
import com.presisco.lazystorm.lifecycle.FlexStreams
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.BasicOutputCollector
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseBasicBolt
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.apache.storm.tuple.Values
import org.slf4j.LoggerFactory

abstract class LazyBasicBolt<out T>(
        private var srcPos: Int = DATA_FIELD_POS,
        private var srcField: String = DATA_FIELD_NAME
) : BaseBasicBolt(), FlexStreams {
    private val logger = LoggerFactory.getLogger(LazyBasicBolt::class.java)

    val customDataStreams = ArrayList<String>()

    override fun addStreams(streams: List<String>) {
        customDataStreams.addAll(streams)
    }

    fun setSrcPos(pos: Int): LazyBasicBolt<T> {
        srcPos = pos
        return this
    }

    fun setSrcField(field: String): LazyBasicBolt<T> {
        srcField = field
        return this
    }

    fun getInput(tuple: Tuple) = if (srcPos != DATA_FIELD_POS)
        tuple.getValue(srcPos) as T
    else
        tuple.getValueByField(srcField) as T

    protected fun Tuple.toData(): T = this.getValueByField(DATA_FIELD_NAME) as T

    override fun prepare(topoConf: MutableMap<String, Any>?, context: TopologyContext?) {
        logger.debug("LazyBasicBolt prepared!")
    }

    fun getArrayListInput(tuple: Tuple): ArrayList<out T> {
        val fuzzy = getInput(tuple)

        return if (fuzzy !is List<*>) {
            arrayListOf(fuzzy)
        } else {
            fuzzy as ArrayList<T>
        }
    }

    protected fun BasicOutputCollector.emitData(data: Any) = this.emit(DATA_STREAM_NAME, Values(data))

    protected fun BasicOutputCollector.emitFailed(data: Any, msg: String, time: String) = this.emit(FAILED_STREAM_NAME, Values(data, msg, time))

    protected fun BasicOutputCollector.emitStats(data: Any, time: String) = this.emit(STATS_STREAM_NAME, Values(data, time))

    protected fun BasicOutputCollector.emitDataToStreams(sourceStream: String, data: Any) = if (sourceStream in customDataStreams) {
        this.emit(sourceStream, Values(data))
    } else {
        this.emitData(data)
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