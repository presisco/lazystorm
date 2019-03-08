package com.presisco.lazystorm

import com.presisco.lazystorm.topology.LazyTopoBuilder
import com.presisco.lazystorm.utils.Tools
import org.apache.storm.Config
import org.apache.storm.LocalCluster
import org.apache.storm.StormSubmitter
import org.apache.storm.generated.StormTopology
import org.apache.storm.topology.IComponent
import org.apache.storm.topology.IRichSpout

open class StormBoot(
        private val createCustomSpout: (String, Map<String, Any?>) -> IRichSpout
        = { name, _ -> throw IllegalStateException("unsupported spout name: $name") },
        private val createCustomBolt: (String, Map<String, Any?>) -> IComponent
        = { name, _ -> throw IllegalStateException("unsupported bolt name: $name") }
) {
    private val builder = LazyTopoBuilder()

    fun buildTopology(config: Map<String, Any?>): StormTopology {
        builder.loadDataSource(config["data_source"] as Map<String, Map<String, String>>)
        builder.loadRedisConfig(config["redis"] as Map<String, Map<String, String>>)
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

        conf.setMaxTaskParallelism(1)
        val cluster = LocalCluster()
        cluster.submitTopology(name, conf, topology)

        Thread.sleep((config["lifetime_minute"] as Double).toLong() * 60 * 1000)

        cluster.shutdown()
    }

    fun clusterUpload(config: Map<String, Any?>) {
        val topology = buildTopology(config)
        val name = config["name"] as String
        if (!Tools.isValidTopologyName(name))
            throw java.lang.IllegalStateException("bad topology name: $name")
        val conf = Config()
        conf.setMaxSpoutPending((config["spout_max_pending"] as Double).toInt())
        conf.setNumWorkers((config["workers"] as Double).toInt())
        StormSubmitter.submitTopologyWithProgressBar(name, conf, topology)
    }
}