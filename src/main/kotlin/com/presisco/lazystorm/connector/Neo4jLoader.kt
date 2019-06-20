package com.presisco.lazystorm.connector

import org.neo4j.driver.v1.AuthTokens
import org.neo4j.driver.v1.Config
import org.neo4j.driver.v1.Driver
import org.neo4j.driver.v1.GraphDatabase

class Neo4jLoader : ConnectorLoader<Driver>() {

    override fun getConnector(): Driver {
        val neo4jConfig = Config.defaultConfig()
        return GraphDatabase.driver(
                config["uri"],
                AuthTokens.basic(config["username"], config["password"]),
                neo4jConfig
        )
    }
}