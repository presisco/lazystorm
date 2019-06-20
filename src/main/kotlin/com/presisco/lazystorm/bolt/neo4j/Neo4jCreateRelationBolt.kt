package com.presisco.lazystorm.bolt.neo4j

import com.presisco.lazystorm.getMap
import org.apache.storm.topology.BasicOutputCollector
import org.apache.storm.tuple.Tuple
import org.neo4j.driver.v1.Statement

class Neo4jCreateRelationBolt : Neo4jResourceBolt<Map<String, *>>() {

    override fun execute(tuple: Tuple, collector: BasicOutputCollector) {
        val relationData = getInput(tuple)
        val relation = relationData.getMap<String, Any>("relation")

        val session = getSession()
        val trans = session.beginTransaction()

        val matchCypher = TwinMatch.fromMap(relationData).buildCypher()
        val createRelationCypher = "create (from)-[${Item.fromMap(relation).buildCreationCypher()}]->(to)"
        trans.run(Statement("$matchCypher\n$createRelationCypher"))
        trans.success()
        session.close()
    }

}