package com.presisco.lazystorm.test

import com.presisco.gsonhelper.ConfigMapHelper
import com.presisco.lazystorm.DATA_STREAM_NAME
import com.presisco.lazystorm.FAILED_STREAM_NAME
import com.presisco.lazystorm.Launch
import com.presisco.lazystorm.STATS_STREAM_NAME
import com.presisco.lazystorm.bolt.LazyBasicBolt
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.BasicOutputCollector
import org.apache.storm.tuple.Values
import org.mockito.Mockito

abstract class LazyBasicBoltTest(launcher: Launch, configPath: String, boltName: String) {
    protected val bolt: LazyBasicBolt<*>

    init {
        val config = ConfigMapHelper().readConfigMap(configPath)
        launcher.prepare(config)
        bolt = launcher.stormBoot.builder.createLazyBolt(boltName, (config["topology"] as Map<String, Map<String, Any>>)[boltName]!!, launcher.createCustomBolt) as LazyBasicBolt<*>
    }

    protected fun BasicOutputCollector.emitData(data: Any) {
        this.emit(DATA_STREAM_NAME, Values(data))
    }

    protected fun BasicOutputCollector.emitFailed(data: Any, msg: String, time: String) {
        this.emit(FAILED_STREAM_NAME, Values(data, msg, time))
    }

    protected fun BasicOutputCollector.emitStats(data: Any, time: String) {
        this.emit(STATS_STREAM_NAME, Values(data, time))
    }

    fun fakeEmptyPrepare() {
        val context = Mockito.mock(TopologyContext::class.java)
        val config = mapOf<String, String>()
        bolt.prepare(config, context)
    }

    fun fakeBasicOutputCollector() = Mockito.mock(BasicOutputCollector::class.java)

    protected fun BasicOutputCollector.verifyEmittedData(data: Any) = Mockito.verify(this).emitData(data)

    protected fun BasicOutputCollector.verifyEmit(stream: String, values: List<*>) = Mockito.verify(this).emit(stream, values)

}