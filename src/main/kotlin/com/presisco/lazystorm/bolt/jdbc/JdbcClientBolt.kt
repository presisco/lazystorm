package com.presisco.lazystorm.bolt.jdbc

import com.presisco.lazyjdbc.client.BaseJdbcClient
import com.presisco.lazystorm.getBoolean
import com.presisco.lazystorm.nowTimeString
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.BasicOutputCollector
import org.apache.storm.tuple.Tuple
import org.slf4j.LoggerFactory

abstract class JdbcClientBolt<CLIENT> : BaseJdbcBolt<Any>() {
    private val logger = LoggerFactory.getLogger(JdbcClientBolt::class.java)

    @Transient
    protected lateinit var jdbcClient: BaseJdbcClient<*>
    private var emitOnException = true

    abstract fun loadJdbcClient(): BaseJdbcClient<*>

    fun getJdbcClient() = jdbcClient as CLIENT

    abstract fun process(boltName: String, streamName: String, data: List<*>, table: String, client: CLIENT, collector: BasicOutputCollector): List<*>

    fun setEmitOnException(flag: Boolean): JdbcClientBolt<CLIENT> {
        emitOnException = flag
        return this
    }

    override fun configure(config: Map<String, *>) {
        super.configure(config)
        setEmitOnException(config.getBoolean("emit_on_failure"))
    }

    override fun prepare(stormConf: MutableMap<Any?, Any?>?, context: TopologyContext?) {
        super.prepare(stormConf, context)
        try {
            jdbcClient = loadJdbcClient()
        } catch (e: Exception) {
            throw IllegalStateException("get connection failed! message: ${e.message}, data source name: ${dataSourceLoader.name}, config: ${dataSourceLoader.config}")
        }
    }

    override fun execute(tuple: Tuple, outputCollector: BasicOutputCollector) {
        val start = System.currentTimeMillis()

        val data = getArrayListInput(tuple)
        val stream = tuple.sourceStreamId
        val table = getTable(stream)
        var output = 0

        try {
            val result = process(tuple.sourceComponent, tuple.sourceStreamId, data as List<*>, table, jdbcClient as CLIENT, outputCollector)
            if (result.isNotEmpty()) {
                outputCollector.emitDataToStreams(stream, result)
            }
            output = result.size
        } catch (e: Exception) {
            if (emitOnException) {
                if (customDataStreams.isNotEmpty()) {
                    outputCollector.emitDataToStreams(
                            stream,
                            data
                    )
                    output = data.size
                } else {
                    outputCollector.emitFailed(
                            data,
                            e.message.toString(),
                            nowTimeString()
                    )
                }
            } else {
                throw e
            }
        } finally {
            val duration = System.currentTimeMillis() - start
            outputCollector.emitStats(
                    hashMapOf(
                            "database" to dataSourceLoader.name,
                            "table" to table,
                            "duration" to duration,
                            "input" to data.size,
                            "output" to output
                    ),
                    nowTimeString()
            )
        }
    }
}