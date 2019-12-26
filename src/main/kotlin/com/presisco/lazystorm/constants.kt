package com.presisco.lazystorm

import org.apache.storm.utils.Utils
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter

const val DATA_STREAM_NAME = Utils.DEFAULT_STREAM_ID
const val DATA_FIELD_NAME = "data"
const val DATA_FIELD_POS = 0

const val DEFAULT_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS"

val systemZoneId = ZoneId.systemDefault()!!

val defaultTimeStampFormat = DateTimeFormatter.ofPattern(DEFAULT_TIME_FORMAT)!!

fun nowTimeString() = defaultTimeStampFormat.format(LocalDateTime.now())!!

const val FAILED_STREAM_NAME = "failed"
const val FAILED_MESSAGE_FIELD = "msg"
const val FAILED_TIME = "timestamp"

const val STATS_STREAM_NAME = "stats"
const val STATS_TIME = "timestamp"