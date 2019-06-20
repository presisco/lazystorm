package com.presisco.lazystorm.bolt.neo4j

import com.presisco.lazystorm.byType
import com.presisco.lazystorm.getList

data class TwinMatch(
        val fromVar: String,
        val fromTags: List<String>,
        val toVar: String,
        val toTags: List<String>,
        val compares: List<Triple<String, String, String>>
) {

    fun buildCypher(): String {
        return "match ($fromVar${fromTags.joinToString(separator = "") { ":$it" }}),\n" +
                "\t($toVar${toTags.joinToString(separator = "") { ":$it" }})\n" +
                "where ${compares.joinToString(separator = "\n\tand ") { "$fromVar.${it.first} ${it.second} $toVar.${it.third}" }}"
    }

    companion object {

        fun fromMap(matchData: Map<String, *>, fromVar: String = "from", toVar: String = "to"): TwinMatch {
            val fromTags = if (matchData.containsKey("from")) {
                matchData.getList<String>("from")
            } else {
                listOf()
            }

            val toTags = if (matchData.containsKey("to")) {
                matchData.getList<String>("to")
            } else {
                listOf()
            }

            val compares = if (matchData.containsKey("compares")) {
                matchData.byType<List<Triple<String, String, String>>>("compares")
            } else {
                listOf()
            }

            return TwinMatch(
                    fromVar,
                    fromTags,
                    toVar,
                    toTags,
                    compares
            )
        }

    }
}