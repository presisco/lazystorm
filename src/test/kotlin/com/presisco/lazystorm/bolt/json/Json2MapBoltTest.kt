package com.presisco.lazystorm.bolt.json

import com.presisco.lazystorm.test.SimpleDataTuple
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.BasicOutputCollector
import org.apache.storm.tuple.Values
import org.junit.Before
import org.junit.Test
import org.mockito.Mockito
import kotlin.test.expect

class Json2MapBoltTest {
    private val jsonBolt = Json2MapBolt()
    private val inputTuple = SimpleDataTuple(listOf("data"), listOf("{name: \"james\", age: 16}"))
    private val outputValues = Values(mapOf(
            "name" to "james",
            "age" to 16.0
    ))

    @Before
    fun prepare() {
        val context = Mockito.mock(TopologyContext::class.java)
        jsonBolt.prepare(mutableMapOf(), context)
    }

    @Test
    fun jsonParseTest() {
        val collector = Mockito.mock(BasicOutputCollector::class.java)
        Mockito.`when`(collector.emit(Mockito.anyList())).thenAnswer {
            expect(outputValues) { it.arguments[0] as Values }
            return@thenAnswer listOf(-1)
        }
        jsonBolt.execute(inputTuple, collector)
    }
}