package com.presisco.lazystorm.bolt.jdbc

import com.presisco.lazyjdbc.client.BaseJdbcClient
import com.presisco.lazystorm.bolt.Constants
import com.presisco.toolbox.time.StopWatch
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.BasicOutputCollector
import org.apache.storm.tuple.Tuple
import org.slf4j.LoggerFactory

abstract class JdbcClientBolt<CLIENT> : BaseJdbcBolt<Any>() {
    private val logger = LoggerFactory.getLogger(JdbcClientBolt::class.java)

    @Transient
    private lateinit var jdbcClient: BaseJdbcClient<*>
    private var emitOnException = true

    abstract fun loadJdbcClient(): BaseJdbcClient<*>

    abstract fun process(data: List<*>, client: CLIENT): List<*>

    fun setEmitOnException(flag: Boolean): JdbcClientBolt<CLIENT> {
        emitOnException = flag
        return this
    }

    override fun prepare(stormConf: MutableMap<Any?, Any?>, context: TopologyContext) {
        super.prepare(stormConf, context)
        try {
            jdbcClient = loadJdbcClient()
        } catch (e: Exception) {
            throw IllegalStateException("get connection failed! message: ${e.message}, data source name: ${dataSourceLoader.name}, config: ${dataSourceLoader.config}")
        }
    }

    override fun execute(tuple: Tuple, outputCollector: BasicOutputCollector) {
        val stopWatch = StopWatch()
        stopWatch.start()

        var data = getInput(tuple)

        if (data !is List<*>) {
            data = listOf(data)
        }

        try {
            val result = process(data as List<*>, jdbcClient as CLIENT)
            if (result.isNotEmpty()) {
                outputCollector.emitData(result)
            }
            val duration = stopWatch.currentDurationFromStart()
            outputCollector.emitStats(
                    hashMapOf(
                            "database" to dataSourceLoader.name,
                            "table" to tableName,
                            "duration" to duration,
                            "total" to data.size,
                            "failed" to result.size
                    ),
                    Constants.getTimeStampString()
            )
        } catch (e: Exception) {
            if (emitOnException) {
                val duration = stopWatch.currentDurationFromStart()
                outputCollector.emitFailed(
                        getInput(tuple),
                        e.message.toString(),
                        Constants.getTimeStampString()
                )
                outputCollector.emitStats(
                        hashMapOf(
                                "database" to dataSourceLoader.name,
                                "table" to tableName,
                                "duration" to duration,
                                "total" to (data as List<*>).size,
                                "failed" to data.size
                        ),
                        Constants.getTimeStampString()
                )
            } else {
                throw e
            }
        }
    }
}