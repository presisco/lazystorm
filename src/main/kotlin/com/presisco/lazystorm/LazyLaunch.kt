package com.presisco.lazystorm

object LazyLaunch : Launch() {

    override val createCustomSpout = { name: String, _: Map<String, Any?> -> throw IllegalStateException("unsupported spout name: $name") }

    override val createCustomBolt = { name: String, _: Map<String, Any?> -> throw IllegalStateException("unsupported bolt name: $name") }

    @JvmStatic
    fun main(args: Array<String>) {
        launch(args)
    }
}