package com.presisco.lazystorm

import java.time.Instant
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

fun <T> Map<String, *>.byType(key: String): T = if (this.containsKey(key)) this[key] as T else throw IllegalStateException("$key not defined in map")

fun Map<String, *>.getInt(key: String) = this.byType<Number>(key).toInt()

fun Map<String, *>.getLong(key: String) = this.byType<Number>(key).toLong()

fun Map<String, *>.getString(key: String) = this.byType<String>(key)

fun Map<String, *>.getBoolean(key: String) = this.byType<Boolean>(key)

fun <K, V> Map<String, *>.getMap(key: String) = this.byType<Map<K, V>>(key)

fun Map<String, *>.getHashMap(key: String) = this.byType<HashMap<String, Any?>>(key)

fun <E> Map<String, *>.getList(key: String) = this.byType<List<E>>(key)

fun <E> Map<String, *>.getArrayList(key: String) = this.byType<ArrayList<E>>(key)

fun Map<String, *>.getListOfMap(key: String) = this[key] as List<Map<String, *>>

fun Map<String, *>.addFieldToNewMap(pair: Pair<String, Any?>): HashMap<String, Any?> {
    val newMap = hashMapOf(pair)
    newMap.putAll(this)
    return newMap
}

inline fun <R, T> List<T>.mapToArrayList(mapFunc: (original: T) -> R): ArrayList<R> {
    val arrayList = ArrayList<R>(this.size)
    this.mapTo(arrayList, mapFunc)
    return arrayList
}

inline fun <R, T> List<T>.mapIndexedToArrayList(mapFunc: (original: T) -> R): ArrayList<R> {
    val arrayList = ArrayList<R>(this.size)
    this.forEach { arrayList.add(mapFunc(it)) }
    return arrayList
}

fun <K, V> Map<String, V>.mapKeyToHashMap(keyMap: (key: String) -> K): HashMap<K, V> {
    val hashMap = hashMapOf<K, V>()
    this.forEach { key, value -> hashMap[keyMap(key)] = value }
    return hashMap
}

fun <Old, New> Map<String, Old>.mapValueToHashMap(valueMap: (value: Old) -> New): HashMap<String, New> {
    val hashMap = hashMapOf<String, New>()
    this.forEach { key, value -> hashMap[key] = valueMap(value) }
    return hashMap
}

fun <T> collectionToArrayList(collection: Collection<T>): ArrayList<T> {
    val arrayList = ArrayList<T>(collection.size)
    arrayList.addAll(collection)
    return arrayList
}

fun String.toSystemMs(format: DateTimeFormatter = defaultTimeStampFormat) = LocalDateTime.parse(this, format).toSystemMs()

fun LocalDateTime.toSystemMs() = this.toInstant(OffsetDateTime.now().offset).toEpochMilli()

fun Long.toLocalDateTime() = LocalDateTime.ofInstant(Instant.ofEpochMilli(this), systemZoneId)