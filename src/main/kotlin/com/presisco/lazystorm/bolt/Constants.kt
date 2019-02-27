package com.presisco.lazystorm.bolt

import java.text.SimpleDateFormat

object Constants {
    const val DATA_STREAM_NAME = "default"
    const val DATA_FIELD_NAME = "data"
    const val DATA_FIELD_POS = 0

    val timeStampFormat = SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS")
    fun getTimeStampString() = timeStampFormat.format(System.currentTimeMillis())

    const val FAILED_STREAM_NAME = "failed"
    const val FAILED_MESSAGE_FIELD = "msg"
    const val FAILED_TIME = "time"

    const val STATS_STREAM_NAME = "stats"
    const val STATS_TIME = "time"
}