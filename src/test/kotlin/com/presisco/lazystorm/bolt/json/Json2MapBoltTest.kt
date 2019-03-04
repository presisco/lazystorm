package com.presisco.lazystorm.bolt.json

import com.presisco.lazystorm.test.LazyBoltTest
import com.presisco.lazystorm.test.SimpleDataTuple
import org.apache.storm.tuple.Values
import org.junit.Before
import org.junit.Test

class Json2MapBoltTest : LazyBoltTest() {
    private val jsonBolt = Json2MapBolt()
    private val inputTuple = SimpleDataTuple(listOf("data"), listOf("{name: \"james\", age: 16}"))
    private val outputValues = Values(mapOf(
            "name" to "james",
            "age" to 16.0
    ))

    @Before
    fun prepare() {
        fakeEmptyPrepare(jsonBolt)
    }

    @Test
    fun jsonParseTest() {
        val collector = fakeBasicOutputCollector()
        jsonBolt.execute(inputTuple, collector)
        collector.verifyEmittedData(outputValues)
    }
}