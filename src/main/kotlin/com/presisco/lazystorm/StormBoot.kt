package com.presisco.lazystorm

import com.presisco.lazystorm.connector.LoaderManager
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

    fun prepareLoaders(config: Map<String, Any?>) {
        setOf("neo4j", "data_source", "redis").forEach {
            val loaderConfigs = if (config.containsKey(it)) {
                config.getMap<String, HashMap<String, String>>(it)
            } else {
                mapOf()
            }
            LoaderManager.addType(it, loaderConfigs)
        }
    }

    fun buildTopology(config: Map<String, Any?>): StormTopology {
        prepareLoaders(config)
        return builder.buildTopology(config["topology"] as Map<String, Map<String, Any>>, createCustomSpout, createCustomBolt)
    }

    fun localLaunch(config: Map<String, Any?>) {
        val topology = buildTopology(config)
        val name = config["name"] as String
        if (!Tools.isValidTopologyName(name))
            throw java.lang.IllegalStateException("bad topology name: $name")
        val conf = Config()
        conf.setFallBackOnJavaSerialization(true)
        conf.setMaxSpoutPending(config.getInt("spout_max_pending"))
        conf.setMessageTimeoutSecs(config.getInt("message_timeout_sec"))

        conf.setMaxTaskParallelism(1)

        try {
            System.setProperty("storm.local.sleeptime", (config.getInt("lifetime_minute") * 60).toString())
        } catch (e: TypeCastException) {
            logger.error("undefined \"lifetime_minute\" in config file!")
        }

        val cluster = LocalCluster()
        cluster.submitTopology(name, conf, topology)
    }

    fun clusterUpload(config: Map<String, Any?>) {
        val topology = buildTopology(config)
        val name = config["name"] as String
        if (!Tools.isValidTopologyName(name))
            throw java.lang.IllegalStateException("bad topology name: $name")
        val conf = Config()
        conf.setMaxSpoutPending(config.getInt("spout_max_pending"))
        conf.setMessageTimeoutSecs(config.getInt("message_timeout_sec"))

        conf.setNumWorkers((config["workers"] as Double).toInt())
        StormSubmitter.submitTopologyWithProgressBar(name, conf, topology)
    }
}