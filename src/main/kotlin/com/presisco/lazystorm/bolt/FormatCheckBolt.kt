package com.presisco.lazystorm.bolt

import com.presisco.datamodel.checker.FlatMapChecker
import com.presisco.lazystorm.nowTimeString
import org.apache.storm.topology.BasicOutputCollector
import org.apache.storm.tuple.Tuple

open class FormatCheckBolt(
        private val checker: FlatMapChecker
) : LazyBasicBolt<Any>() {

    override fun execute(tuple: Tuple, collector: BasicOutputCollector) {
        val data = getArrayListInput(tuple)

        val passed = arrayListOf<Map<String, *>>()
        val failed = arrayListOf<Map<String, *>>()

        (data as List<*>).forEach {
            if (checker.checkAny(it).first) {
                passed.add(it as Map<String, *>)
            } else {
                failed.add(it as Map<String, *>)
            }
        }

        if (passed.isNotEmpty()) {
            collector.emitData(passed)
        }
        if (failed.isNotEmpty()) {
            collector.emitFailed(failed, "bad format", nowTimeString())
        }
    }
}