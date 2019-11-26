package com.presisco.lazystorm.lifecycle

interface Connectable<T> {

    fun connect(loader: T)

}