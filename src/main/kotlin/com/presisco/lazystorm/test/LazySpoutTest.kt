package com.presisco.lazystorm.test

import com.presisco.gsonhelper.ConfigMapHelper
import com.presisco.lazystorm.DATA_STREAM_NAME
import com.presisco.lazystorm.FAILED_STREAM_NAME
import com.presisco.lazystorm.Launch
import com.presisco.lazystorm.STATS_STREAM_NAME
import com.presisco.lazystorm.topology.LazyTopoBuilder
import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.IRichSpout
import org.apache.storm.tuple.Values
import org.mockito.Mockito

abstract class LazySpoutTest(launcher: Launch, configPath: String, spoutName: String) {
    protected val spout: IRichSpout

    init {
        val config = ConfigMapHelper().readConfigMap(configPath)
        val builder = LazyTopoBuilder()
        builder.loadDataSource(config["data_source"] as Map<String, Map<String, String>>)
        builder.loadRedisConfig(config["redis"] as Map<String, Map<String, String>>)

        spout = builder.createLazySpout(spoutName, (config["topology"] as Map<String, Map<String, Any>>)[spoutName]!!, launcher.createCustomSpout)
    }

    protected fun SpoutOutputCollector.emitData(data: Any) {
        this.emit(DATA_STREAM_NAME, Values(data))
    }

    protected fun SpoutOutputCollector.emitFailed(data: Any, msg: String, time: String) {
        this.emit(FAILED_STREAM_NAME, Values(data, msg, time))
    }

    protected fun SpoutOutputCollector.emitStats(data: Any, time: String) {
        this.emit(STATS_STREAM_NAME, Values(data, time))
    }

    fun fakeEmptyPrepare(): SpoutOutputCollector {
        val context = Mockito.mock(TopologyContext::class.java)
        val config = mutableMapOf<String, String>()
        val collector = fakeOutputCollector()
        spout.open(config, context, collector)
        return collector
    }

    fun fakeOutputCollector() = Mockito.mock(SpoutOutputCollector::class.java)

    protected fun SpoutOutputCollector.verifyEmittedData(data: Any) = Mockito.verify(this).emitData(data)

    protected fun SpoutOutputCollector.verifyEmit(stream: String, values: List<*>) = Mockito.verify(this).emit(stream, values)
}