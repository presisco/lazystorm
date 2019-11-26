package com.presisco.lazystorm.bolt.jdbc

import com.presisco.lazystorm.LazyLaunch
import com.presisco.lazystorm.test.LazyBasicBoltTest
import com.presisco.lazystorm.test.SimpleDataTuple
import org.junit.Before
import org.junit.Test

class SimpleInsertBoltTest : LazyBasicBoltTest(LazyLaunch, "sample/config.json", "simple_insert_bolt") {

    @Before
    fun prepare() {
        fakeEmptyPrepare()
    }

    @Test
    fun insertFailureTest() {
        val inputTuple = SimpleDataTuple(listOf("data"), listOf(
                mapOf(
                        "id" to 1,
                        "sid" to "unknown",
                        "recordtime" to "when",
                        "tep" to 111111
                )
        ), stream = "one")
        val outputValues = mapOf(
                "name" to "james",
                "age" to 16.0
        )
        val collector = fakeBasicOutputCollector()
        bolt.execute(inputTuple, collector)
        collector.verifyEmittedData(outputValues)
    }
}