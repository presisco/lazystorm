package com.presisco.lazystorm.bolt.neo4j

import com.presisco.lazystorm.LazyLaunch
import com.presisco.lazystorm.test.LazyBasicBoltTest
import com.presisco.lazystorm.test.SimpleDataTuple
import org.junit.Before
import org.junit.Test

class Neo4jCreateNodeBoltTest : LazyBasicBoltTest(LazyLaunch, "sample/config.json", "neo4j_create_node_bolt") {

    @Before
    fun prepare() {
        fakeEmptyPrepare()
    }

    @Test
    fun createNodeTest() {
        val collector = fakeBasicOutputCollector()
        bolt.execute(SimpleDataTuple("data" to mapOf(
                "tags" to listOf("person", "teacher"),
                "props" to mapOf(
                        "name" to "james",
                        "age" to 30
                )
        )), collector)
        bolt.execute(SimpleDataTuple("data" to mapOf(
                "tags" to listOf("person", "worker"),
                "props" to mapOf(
                        "name" to "tom",
                        "age" to 26
                )
        )), collector)
    }
}