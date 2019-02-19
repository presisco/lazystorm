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

    private lateinit var stormBoot: StormBoot

    protected fun <T> Map<String, *>.byType(key: String): T = if (this.containsKey(key)) this[key] as T else throw IllegalStateException("$key not defined in config")

    protected fun Map<String, *>.getInt(key: String) = this.byType<Number>(key).toInt()

    protected fun Map<String, *>.getLong(key: String) = this.byType<Number>(key).toLong()

    protected fun Map<String, *>.getString(key: String) = this.byType<String>(key)

    protected fun Map<String, *>.getBoolean(key: String) = this.byType<Boolean>(key)

    protected fun <K, V> Map<String, *>.getHashMap(key: String) = this.byType<java.util.HashMap<K, V>>(key)

    protected fun <E> Map<String, *>.getList(key: String) = this.byType<List<E>>(key)

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

    fun getDataSourceLoader(name: String) = stormBoot.getDataSourceLoader(name)

    fun launch(args: Array<String>) {
        parseArgs(args)

        stormBoot = StormBoot(createCustomSpout, createCustomBolt)
        val config = ConfigMapHelper().readConfigMap(cmdArgs["config"] as String)
        when (cmdArgs["mode"]) {
            "local" -> stormBoot.localLaunch(config)
            "cluster" -> stormBoot.clusterUpload(config)
            else -> throw IllegalStateException("unsupported launch mode: ${cmdArgs["mode"]}")
        }
    }
}