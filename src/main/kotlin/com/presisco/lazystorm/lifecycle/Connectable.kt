package com.presisco.lazystorm.lifecycle

interface Connectable<T> {

    fun connect(connector: T)

}