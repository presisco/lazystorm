package com.presisco.lazystorm.test

import org.apache.storm.tuple.Tuple
import org.apache.storm.windowing.TupleWindow

class SimpleTupleWindow(
        private val tuples: MutableList<Tuple>,
        private val expired: MutableList<Tuple> = mutableListOf(),
        private val new: MutableList<Tuple> = mutableListOf()
) : TupleWindow {
    override fun getEndTimestamp() = 1L

    override fun getStartTimestamp() = 0L

    override fun getNew(): MutableList<Tuple> = new

    override fun getExpired(): MutableList<Tuple> = expired

    override fun get(): MutableList<Tuple> = tuples

}