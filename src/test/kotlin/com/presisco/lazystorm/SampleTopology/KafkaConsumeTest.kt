package com.presisco.lazystorm.SampleTopology

import com.presisco.lazystorm.LazyLaunch
import org.junit.Test

class KafkaConsumeTest {

    @Test
    fun run() {
        LazyLaunch.main(arrayOf(
                "config=sample/kafka-dump.json",
                "mode=local"
        ))
    }
}