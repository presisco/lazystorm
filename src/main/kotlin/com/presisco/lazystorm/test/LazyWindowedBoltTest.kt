package com.presisco.lazystorm.test

import com.presisco.gsonhelper.ConfigMapHelper
import com.presisco.lazystorm.DATA_STREAM_NAME
import com.presisco.lazystorm.FAILED_STREAM_NAME
import com.presisco.lazystorm.Launch
import com.presisco.lazystorm.STATS_STREAM_NAME
import com.presisco.lazystorm.bolt.LazyWindowedBolt
import com.presisco.lazystorm.topology.LazyTopoBuilder
import org.apache.storm.task.OutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.tuple.Values
import org.mockito.Mockito

abstract class LazyWindowedBoltTest(launcher: Launch, configPath: String, boltName: String) {
    protected val bolt: LazyWindowedBolt<*>

    init {
        val config = ConfigMapHelper().readConfigMap(configPath)
        val builder = LazyTopoBuilder()

        bolt = builder.createLazyBolt(boltName, (config["topology"] as Map<String, Map<String, Any>>)[boltName]!!, launcher.createCustomBolt) as LazyWindowedBolt<*>
    }

    protected fun OutputCollector.emitData(data: Any) {
        this.emit(DATA_STREAM_NAME, Values(data))
    }

    protected fun OutputCollector.emitFailed(data: Any, msg: String, time: String) {
        this.emit(FAILED_STREAM_NAME, Values(data, msg, time))
    }

    protected fun OutputCollector.emitStats(data: Any, time: String) {
        this.emit(STATS_STREAM_NAME, Values(data, time))
    }

    fun fakeEmptyPrepare(): OutputCollector {
        val context = Mockito.mock(TopologyContext::class.java)
        val config = mutableMapOf<Any?, Any?>()
        val collector = fakeOutputCollector()
        bolt.prepare(config, context, collector)
        return collector
    }

    fun fakeOutputCollector() = Mockito.mock(OutputCollector::class.java)

    protected fun OutputCollector.verifyEmittedData(data: Any) = Mockito.verify(this).emitData(data)

    protected fun OutputCollector.verifyEmit(stream: String, values: List<*>) = Mockito.verify(this).emit(stream, values)

}