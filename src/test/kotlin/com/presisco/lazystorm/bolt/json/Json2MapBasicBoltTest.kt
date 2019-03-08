package com.presisco.lazystorm.bolt.json

import com.presisco.lazystorm.LazyLaunch
import com.presisco.lazystorm.test.LazyBasicBoltTest
import com.presisco.lazystorm.test.SimpleDataTuple
import org.junit.Before
import org.junit.Test

class Json2MapBasicBoltTest : LazyBasicBoltTest(LazyLaunch, "sample/config.json", "json_2_map_bolt") {

    @Before
    fun prepare() {
        fakeEmptyPrepare()
    }

    @Test
    fun jsonParseTest() {
        val inputTuple = SimpleDataTuple(listOf("data"), listOf("{name: \"james\", age: 16}"))
        val outputValues = mapOf(
                "name" to "james",
                "age" to 16.0
        )
        val collector = fakeBasicOutputCollector()
        bolt.execute(inputTuple, collector)
        collector.verifyEmittedData(outputValues)
    }
}