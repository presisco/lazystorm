package com.presisco.lazystorm

import com.presisco.gsonhelper.ConfigMapHelper
import org.apache.storm.topology.IComponent
import org.apache.storm.topology.IRichSpout

abstract class Launch {

    abstract val createCustomSpout: (String, Map<String, Any?>) -> IRichSpout

    abstract val createCustomBolt: (String, Map<String, Any?>) -> IComponent

    private val cmdArgs: MutableMap<String, String> = mutableMapOf(
            "config" to "sample/config.json",
            "mode" to "cluster"
    )

    fun String.getKey() = this.substringBefore('=')
    fun String.getValue() = this.substringAfter('=')

    lateinit var stormBoot: StormBoot

    /**
     * 解析命令行运行参数
     */
    fun parseArgs(args: Array<String>) {
        for (arg in args) {
            with(arg) {
                cmdArgs[getKey()] = getValue()
            }
        }
    }

    fun prepare(config: Map<String, Any?>) {
        stormBoot = StormBoot(createCustomSpout, createCustomBolt)
        stormBoot.prepareLoaders(config)
    }

    fun launch(args: Array<String>, onConnectorCreated: ((builder: LazyTopoBuilder) -> Unit)? = null) {
        parseArgs(args)

        val config = ConfigMapHelper().readConfigMap(cmdArgs["config"] as String)
        prepare(config)
        if (onConnectorCreated != null) {
            onConnectorCreated(stormBoot.builder)
        }
        when (cmdArgs["mode"]) {
            "local" -> stormBoot.localLaunch(config)
            "cluster" -> stormBoot.clusterUpload(config)
            else -> throw IllegalStateException("unsupported launch mode: ${cmdArgs["mode"]}")
        }
    }
}