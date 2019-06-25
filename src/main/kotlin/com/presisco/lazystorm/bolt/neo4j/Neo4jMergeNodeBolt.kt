package com.presisco.lazystorm.bolt.neo4j

import com.presisco.lazystorm.lifecycle.Configurable
import org.apache.storm.topology.BasicOutputCollector
import org.apache.storm.tuple.Tuple
import org.neo4j.driver.v1.Statement

class Neo4jMergeNodeBolt : Neo4jResourceBolt<Map<String, *>>(), Configurable {

    override fun configure(config: Map<String, *>) {

    }

    override fun execute(tuple: Tuple, collector: BasicOutputCollector) {
        val nodeData = getInput(tuple)
        val session = getSession()
        val trans = session.beginTransaction()
        val nodeCypher = Item.fromMap(nodeData).buildCreationCypher()
        trans.run(Statement("merge ($nodeCypher)"))
        trans.success()
        session.close()
    }
}