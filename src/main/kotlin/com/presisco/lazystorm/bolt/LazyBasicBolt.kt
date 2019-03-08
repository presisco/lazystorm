package com.presisco.lazystorm.bolt

import org.apache.storm.topology.BasicOutputCollector
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseBasicBolt
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.apache.storm.tuple.Values
import org.slf4j.LoggerFactory

abstract class LazyBasicBolt<out T>(
        private var srcPos: Int = Constants.DATA_FIELD_POS,
        private var srcField: String = Constants.DATA_FIELD_NAME
) : BaseBasicBolt() {
    private val logger = LoggerFactory.getLogger(LazyBasicBolt::class.java)

    var customDataStreams = ArrayList<String>()

    fun setSrcPos(pos: Int): LazyBasicBolt<T> {
        srcPos = pos
        return this
    }

    fun setSrcField(field: String): LazyBasicBolt<T> {
        srcField = field
        return this
    }
    
    fun getInput(tuple: Tuple) = if (srcPos != Constants.DATA_FIELD_POS)
        tuple.getValue(srcPos) as T
    else
        tuple.getValueByField(srcField) as T

    fun getArrayListInput(tuple: Tuple): ArrayList<out T> {
        val fuzzy = getInput(tuple)

        return if (fuzzy !is List<*>) {
            arrayListOf(fuzzy)
        } else {
            fuzzy as ArrayList<T>
        }
    }

    protected fun BasicOutputCollector.emitData(data: Any) = this.emit(Constants.DATA_STREAM_NAME, Values(data))

    protected fun BasicOutputCollector.emitFailed(data: Any, msg: String, time: String) = this.emit(Constants.FAILED_STREAM_NAME, Values(data, msg, time))

    protected fun BasicOutputCollector.emitStats(data: Any, time: String) = this.emit(Constants.STATS_STREAM_NAME, Values(data, time))

    protected fun BasicOutputCollector.emitDataToStreams(sourceStream: String, data: Any) = if (sourceStream in customDataStreams) {
        this.emit(sourceStream, Values(data))
    } else {
        this.emitData(data)
    }

    protected fun <T> Map<String, *>.byType(key: String): T = if (this.containsKey(key)) this[key] as T else throw IllegalStateException("$key not defined in config")

    protected fun Map<String, *>.getInt(key: String) = this.byType<Number>(key).toInt()

    protected fun Map<String, *>.getLong(key: String) = this.byType<Number>(key).toLong()

    protected fun Map<String, *>.getString(key: String) = this.byType<String>(key)

    protected fun Map<String, *>.getBoolean(key: String) = this.byType<Boolean>(key)

    protected fun <K, V> Map<String, *>.getMap(key: String) = this.byType<Map<K, V>>(key)

    protected fun Map<String, *>.getHashMap(key: String) = this.byType<HashMap<String, Any?>>(key)

    protected fun <E> Map<String, *>.getList(key: String) = this.byType<List<E>>(key)

    protected fun <E> Map<String, *>.getArrayList(key: String) = this.byType<ArrayList<E>>(key)

    protected fun Map<String, *>.getListOfMap(key: String) = this[key] as List<Map<String, *>>

    protected fun <K, V> Map<String, V>.mapKeyToHashMap(keyMap: (key: String) -> K): HashMap<K, V> {
        val hashMap = hashMapOf<K, V>()
        this.forEach { key, value -> hashMap[keyMap(key)] = value }
        return hashMap
    }

    protected fun <Old, New> Map<String, Old>.mapValueToHashMap(valueMap: (value: Old) -> New): HashMap<String, New> {
        val hashMap = hashMapOf<String, New>()
        this.forEach { key, value -> hashMap[key] = valueMap(value) }
        return hashMap
    }

    override fun declareOutputFields(declarer: OutputFieldsDeclarer) {
        declarer.declareStream(
                Constants.DATA_STREAM_NAME,
                Fields(Constants.DATA_FIELD_NAME))
        declarer.declareStream(
                Constants.STATS_STREAM_NAME,
                Fields(
                        Constants.DATA_FIELD_NAME,
                        Constants.STATS_TIME
                )
        )
        declarer.declareStream(
                Constants.FAILED_STREAM_NAME,
                Fields(
                        Constants.DATA_FIELD_NAME,
                        Constants.FAILED_MESSAGE_FIELD,
                        Constants.FAILED_TIME
                )
        )
        customDataStreams.filter {
            it !in setOf(
                    Constants.DATA_STREAM_NAME,
                    Constants.STATS_STREAM_NAME,
                    Constants.FAILED_STREAM_NAME
            )
        }.forEach {
            declarer.declareStream(it, Fields(Constants.DATA_FIELD_NAME))
        }
    }
}