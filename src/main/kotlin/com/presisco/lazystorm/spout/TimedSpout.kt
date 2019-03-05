package com.presisco.lazystorm.spout

import com.presisco.lazystorm.bolt.Constants
import com.presisco.toolbox.time.StopWatch
import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichSpout
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Values

abstract class TimedSpout : BaseRichSpout() {
    private var intervalSec: Long = 60

    @Transient
    private lateinit var collector: SpoutOutputCollector

    abstract fun producer(): HashMap<String, *>?

    fun setIntervalSec(sec: Long): TimedSpout {
        intervalSec = sec
        return this
    }

    override fun nextTuple() {
        val stopWatch = StopWatch()
        stopWatch.start()
        val data = producer()
        data?.let {
            collector.emit(Values(it))
        }
        stopWatch.stop()
        val duration = stopWatch.durationFromStart()
        if (duration < intervalSec) {
            Thread.sleep(intervalSec - duration)
        }
    }

    override fun open(config: MutableMap<*, *>, context: TopologyContext, collector: SpoutOutputCollector) {
        this.collector = collector
    }

    override fun declareOutputFields(declarer: OutputFieldsDeclarer) {
        declarer.declare(
                Fields(Constants.DATA_FIELD_NAME)
        )
    }
}