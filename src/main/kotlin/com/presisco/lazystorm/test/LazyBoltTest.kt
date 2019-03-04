package com.presisco.lazystorm.test

import com.presisco.lazystorm.bolt.Constants
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.BasicOutputCollector
import org.apache.storm.topology.base.BaseBasicBolt
import org.apache.storm.tuple.Values
import org.mockito.Mockito

abstract class LazyBoltTest {

    protected fun BasicOutputCollector.emitData(data: Any) {
        this.emit(Constants.DATA_STREAM_NAME, Values(data))
    }

    protected fun BasicOutputCollector.emitFailed(data: Any, msg: String, time: String) {
        this.emit(Constants.FAILED_STREAM_NAME, Values(data, msg, time))
    }

    protected fun BasicOutputCollector.emitStats(data: Any, time: String) {
        this.emit(Constants.STATS_STREAM_NAME, Values(data, time))
    }

    fun fakeEmptyPrepare(bolt: BaseBasicBolt) {
        val context = Mockito.mock(TopologyContext::class.java)
        val config = mapOf<String, String>()
        bolt.prepare(config, context)
    }

    fun fakeBasicOutputCollector() = Mockito.mock(BasicOutputCollector::class.java)

    protected fun BasicOutputCollector.verifyEmittedData(data: Any) = Mockito.verify(this).emitData(data)

    protected fun BasicOutputCollector.verifyEmit(stream: String, values: List<*>) = Mockito.verify(this).emit(stream, values)

}