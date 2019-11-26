package com.presisco.lazystorm.bolt.neo4j

import com.presisco.lazystorm.getList
import com.presisco.lazystorm.getMap

data class Item(
        val cypherVar: String,
        val tags: List<String>,
        val props: Map<String, Any?>
) {

    fun buildPropsCypher(): String {
        val content = props.entries.joinToString(separator = ",\n") {
            val valueText = when (it.value) {
                is Number -> it.value.toString()
                else -> {
                    if (it.value.toString().startsWith("$")) {
                        it.value.toString()
                    } else {
                        "'${it.value}'"
                    }
                }
            }
            "${it.key}: $valueText"
        }
        return "{$content}"
    }

    fun buildTagsCypher() = tags.joinToString(separator = "") { ":$it" }

    fun buildCreationCypher(): String {
        return "$cypherVar${buildTagsCypher()} ${buildPropsCypher()}"
    }

    companion object {
        fun fromMap(itemData: Map<String, *>, cypherVar: String = ""): Item {
            val tags = if (itemData.containsKey("tags")) {
                itemData.getList<String>("tags")
            } else {
                listOf()
            }

            val props = if (itemData.containsKey("props")) {
                itemData.getMap<String, Any?>("props")
            } else {
                mapOf()
            }

            return Item(cypherVar, tags, props)
        }
    }
}