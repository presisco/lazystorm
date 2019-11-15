package com.presisco.lazystorm

import com.presisco.lazystorm.topology.LazyTopoBuilder
import com.presisco.lazystorm.utils.Tools
import org.apache.storm.Config
import org.apache.storm.LocalCluster
import org.apache.storm.StormSubmitter
import org.apache.storm.generated.StormTopology
import org.apache.storm.topology.IComponent
import org.apache.storm.topology.IRichSpout
import org.slf4j.LoggerFactory

open class StormBoot(
        private val createCustomSpout: (String, Map<String, Any?>) -> IRichSpout
        = { name, _ -> throw IllegalStateException("unsupported spout name: $name") },
        private val createCustomBolt: (String, Map<String, Any?>) -> IComponent
        = { name, _ -> throw IllegalStateException("unsupported bolt name: $name") }
) {
    private val logger = LoggerFactory.getLogger(StormBoot::class.java)
    val builder = LazyTopoBuilder()

    fun loadDataSource(config: Map<String, Any?>) {
        if (config.containsKey("data_source")) {
            builder.loadDataSource(config["data_source"] as Map<String, Map<String, String>>)
        }
        if (config.containsKey("redis")) {
            builder.loadRedisConfig(config["redis"] as Map<String, Map<String, String>>)
        }
    }

    fun buildTopology(config: Map<String, Any?>): StormTopology {
        return builder.buildTopology(config["topology"] as Map<String, Map<String, Any>>, createCustomSpout, createCustomBolt)
    }

    fun getDataSourceLoader(name: String) = builder.getDataSourceLoader(name)

    fun localLaunch(config: Map<String, Any?>) {
        val topology = buildTopology(config)
        val name = config["name"] as String
        if (!Tools.isValidTopologyName(name))
            throw java.lang.IllegalStateException("bad topology name: $name")
        val conf = Config()
        conf.setFallBackOnJavaSerialization(true)
        conf.setMaxSpoutPending((config["spout_max_pending"] as Double).toInt())
        conf.setMessageTimeoutSecs((config["message_timeout_sec"] as Double).toInt())

        conf.setMaxTaskParallelism(1)
        val cluster = LocalCluster()
        cluster.submitTopology(name, conf, topology)

        try {
            Thread.sleep((config["lifetime_minute"] as Double).toLong() * 60 * 1000)
        } catch (e: TypeCastException) {
            logger.error("undefined \"lifetime_minute\" in config file!")
        }

        cluster.shutdown()
    }

    fun clusterUpload(config: Map<String, Any?>) {
        val topology = buildTopology(config)
        val name = config["name"] as String
        if (!Tools.isValidTopologyName(name))
            throw java.lang.IllegalStateException("bad topology name: $name")
        val conf = Config()
        conf.setMaxSpoutPending((config["spout_max_pending"] as Double).toInt())
        conf.setMessageTimeoutSecs((config["message_timeout_sec"] as Double).toInt())

        conf.setNumWorkers((config["workers"] as Double).toInt())
        StormSubmitter.submitTopologyWithProgressBar(name, conf, topology)
    }
}