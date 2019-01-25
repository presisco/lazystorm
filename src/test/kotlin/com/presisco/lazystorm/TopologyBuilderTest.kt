package com.presisco.lazystorm

import com.presisco.gsonhelper.ConfigMapHelper
import org.junit.Test

class TopologyBuilderTest {

    @Test
    fun configParse() {
        val config = ConfigMapHelper().readConfigMap("sample/config.json")
        val boot = StormBoot()
        boot.buildTopology(config)
    }
}