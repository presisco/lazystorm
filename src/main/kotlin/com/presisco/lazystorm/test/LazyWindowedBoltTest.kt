package com.presisco.lazystorm.test

import com.presisco.gsonhelper.ConfigMapHelper
import com.presisco.lazystorm.Launch
import com.presisco.lazystorm.bolt.Constants
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
        builder.loadDataSource(config["data_source"] as Map<String, Map<String, String>>)
        builder.loadRedisConfig(config["redis"] as Map<String, Map<String, String>>)

        bolt = builder.createLazyBolt(boltName, (config["topology"] as Map<String, Map<String, Any>>)[boltName]!!, launcher.createCustomBolt) as LazyWindowedBolt<*>
    }

    protected fun OutputCollector.emitData(data: Any) {
        this.emit(Constants.DATA_STREAM_NAME, Values(data))
    }

    protected fun OutputCollector.emitFailed(data: Any, msg: String, time: String) {
        this.emit(Constants.FAILED_STREAM_NAME, Values(data, msg, time))
    }

    protected fun OutputCollector.emitStats(data: Any, time: String) {
        this.emit(Constants.STATS_STREAM_NAME, Values(data, time))
    }

    fun fakeEmptyPrepare(): OutputCollector {
        val context = Mockito.mock(TopologyContext::class.java)
        val config = mutableMapOf<String, String>()
        val collector = fakeOutputCollector()
        bolt.prepare(config, context, collector)
        return collector
    }

    fun fakeOutputCollector() = Mockito.mock(OutputCollector::class.java)

    protected fun OutputCollector.verifyEmittedData(data: Any) = Mockito.verify(this).emitData(data)

    protected fun OutputCollector.verifyEmit(stream: String, values: List<*>) = Mockito.verify(this).emit(stream, values)

}