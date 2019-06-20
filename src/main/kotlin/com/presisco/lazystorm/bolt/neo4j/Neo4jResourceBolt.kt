package com.presisco.lazystorm.bolt.neo4j

import com.presisco.lazystorm.bolt.LazyBasicBolt
import com.presisco.lazystorm.connector.Neo4jLoader
import com.presisco.lazystorm.connector.Neo4jManager
import com.presisco.lazystorm.lifecycle.Connectable
import org.apache.storm.task.TopologyContext
import org.neo4j.driver.v1.Driver

abstract class Neo4jResourceBolt<T> : LazyBasicBolt<T>(), Connectable<Neo4jLoader> {

    @Transient
    protected lateinit var driver: Driver

    private lateinit var loader: Neo4jLoader

    override fun connect(connector: Neo4jLoader) {
        loader = connector
    }

    override fun prepare(topoConf: Map<String, *>, context: TopologyContext) {
        driver = Neo4jManager.getConnector(loader)
    }

    protected fun getSession() = driver.session()

}