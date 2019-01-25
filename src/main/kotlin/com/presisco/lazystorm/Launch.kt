package com.presisco.lazystorm

import com.presisco.gsonhelper.ConfigMapHelper
import org.apache.storm.topology.IRichSpout

abstract class Launch {

    abstract val createCustomSpout: (String, Map<String, Any?>) -> IRichSpout

    abstract val createCustomBolt: (String, Map<String, Any?>) -> Any

    private val cmdArgs: MutableMap<String, String> = mutableMapOf(
            "config" to "sample/config.json",
            "mode" to "cluster"
    )

    fun String.getKey() = this.substringBefore('=')
    fun String.getValue() = this.substringAfter('=')

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

    fun launch(args: Array<String>) {
        parseArgs(args)

        val config = ConfigMapHelper().readConfigMap(cmdArgs["config"] as String)
        val boot = StormBoot(createCustomSpout, createCustomBolt)
        when (cmdArgs["mode"]) {
            "local" -> boot.localLaunch(config)
            "cluster" -> boot.clusterUpload(config)
            else -> throw IllegalStateException("unsupported launch mode: ${cmdArgs["mode"]}")
        }
    }
}