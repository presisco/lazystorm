package com.presisco.lazystorm.topology

import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.tuple.Fields
import org.apache.storm.utils.Utils

class OutputScanner : OutputFieldsDeclarer {
    companion object {
        const val DEFAULT_STREAM = Utils.DEFAULT_STREAM_ID
    }

    val streams = hashMapOf<String, List<String>>()
    val directs = hashSetOf<String>()

    override fun declareStream(stream: String, fields: Fields) {
        streams[stream] = fields.toList()
    }

    override fun declareStream(stream: String, direct: Boolean, fields: Fields) {
        declareStream(stream, fields)
        if (direct) {
            directs.add(stream)
        }
    }

    override fun declare(fields: Fields) {
        streams[DEFAULT_STREAM] = fields.toList()
    }

    override fun declare(direct: Boolean, fields: Fields) {
        declare(fields)
        if (direct) {
            directs.add(DEFAULT_STREAM)
        }
    }

}