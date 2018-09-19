package com.presisco.lazystorm.utils

object Tools {
    val illegalCharRegEx = Regex("[^\\w\\-]")

    fun isValidTopologyName(name: String) = !name.contains(illegalCharRegEx)
}