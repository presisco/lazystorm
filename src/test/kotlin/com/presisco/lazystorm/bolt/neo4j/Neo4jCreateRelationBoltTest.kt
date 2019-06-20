package com.presisco.lazystorm.bolt.neo4j

import com.presisco.lazystorm.LazyLaunch
import com.presisco.lazystorm.test.LazyBasicBoltTest
import com.presisco.lazystorm.test.SimpleDataTuple
import org.junit.Before
import org.junit.Test

class Neo4jCreateRelationBoltTest : LazyBasicBoltTest(LazyLaunch, "sample/config.json", "neo4j_create_relation_bolt") {

    @Before
    fun prepare() {
        fakeEmptyPrepare()
    }

    @Test
    fun createNodeTest() {
        val inputTuple = SimpleDataTuple("data" to mapOf(
                "relation" to mapOf(
                        "tags" to listOf("father"),
                        "props" to mapOf(
                                "description" to "for jokes"
                        )
                ),
                "from" to listOf("person"),
                "to" to listOf("person"),
                "compares" to listOf(Triple("age", ">", "age"))
        ))
        val collector = fakeBasicOutputCollector()
        bolt.execute(inputTuple, collector)
    }
}