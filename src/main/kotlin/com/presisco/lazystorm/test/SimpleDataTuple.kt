package com.presisco.lazystorm.test

import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple

open class SimpleDataTuple(
        private val keys: List<String>,
        private val values: List<*>,
        private val component: String = "unknown",
        private val stream: String = "default",
        private val task: Int = 1
) : Tuple {
    private val dataMap = HashMap<String, Any?>()

    constructor(
            vararg entries: Pair<String, *>,
            component: String = "unknown",
            stream: String = "default",
            task: Int = 1
    ) : this(
            entries.map { it.first },
            entries.map { it.second },
            component,
            stream,
            task
    )

    init {
        keys.forEachIndexed { index, key -> dataMap[key] = values[index] }
    }

    override fun contains(key: String) = keys.contains(key)

    override fun getBooleanByField(key: String) = dataMap[key] as Boolean

    override fun getBinaryByField(key: String) = dataMap[key] as ByteArray

    override fun getFields() = Fields(keys)

    override fun getStringByField(key: String) = dataMap[key] as String

    override fun getIntegerByField(key: String) = dataMap[key] as Int

    override fun getByteByField(key: String) = dataMap[key] as Byte

    override fun getFloatByField(key: String) = dataMap[key] as Float

    override fun getLong(index: Int) = values[index] as Long

    override fun getMessageId() = null

    override fun getFloat(index: Int) = values[index] as Float

    override fun getSourceGlobalStreamId() = null

    override fun getSourceComponent() = component

    override fun size() = keys.size

    override fun getValue(index: Int) = values[index]

    override fun getDouble(index: Int) = values[index] as Double

    override fun getLongByField(key: String) = dataMap[key] as Long

    override fun getBoolean(index: Int) = values[index] as Boolean

    override fun select(fields: Fields): List<Any> {
        val valueList = ArrayList<Any>(fields.size())
        fields.forEach { valueList.add(dataMap[it]!!) }
        return valueList
    }

    override fun getDoubleByField(key: String) = dataMap[key] as Double

    override fun getInteger(index: Int) = values[index] as Int

    override fun getSourceTask() = task

    override fun getBinary(index: Int) = values[index] as ByteArray

    override fun getValueByField(key: String) = dataMap[key]

    override fun getShortByField(key: String) = dataMap[key] as Short

    override fun getContext() = null

    override fun fieldIndex(key: String) = keys.indexOf(key)

    override fun getValues() = values

    override fun getSourceStreamId() = stream

    override fun getShort(index: Int) = values[index] as Short

    override fun getByte(index: Int) = values[index] as Byte

    override fun getString(index: Int) = values[index] as String

}