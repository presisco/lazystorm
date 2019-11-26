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

    override fun connect(loader: Neo4jLoader) {
        this.loader = loader
    }

    override fun prepare(stormConf: MutableMap<Any?, Any?>?, context: TopologyContext?) {
        driver = Neo4jManager.getConnector(loader)
    }

    protected fun getSession() = driver.session()

}