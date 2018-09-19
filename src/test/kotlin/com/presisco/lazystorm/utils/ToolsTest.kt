package com.presisco.lazystorm.utils

import org.junit.Test
import kotlin.test.expect

class ToolsTest {

    @Test
    fun topologyNamingTest() {
        expect(true) { Tools.isValidTopologyName("sample") }
        expect(true) { Tools.isValidTopologyName("sample-topology") }
        expect(true) { Tools.isValidTopologyName("sample_topology") }
        expect(false) { Tools.isValidTopologyName("sample topology") }
        expect(false) { Tools.isValidTopologyName("sample%topology") }
        expect(false) { Tools.isValidTopologyName("sample\$topology") }
        expect(false) { Tools.isValidTopologyName("sample:topology") }
    }
}