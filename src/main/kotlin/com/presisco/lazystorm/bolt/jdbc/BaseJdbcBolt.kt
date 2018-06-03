package com.presisco.lazystorm.bolt.jdbc

import com.presisco.lazystorm.bolt.Constants
import com.presisco.lazystorm.bolt.LazyBasicBolt
import org.apache.storm.topology.base.BaseBasicBolt
import javax.sql.DataSource

abstract class BaseJdbcBolt<out T>(
        srcPos: Int,
        srcField: String,
        protected val dataSource: DataSource,
        protected val tableName: String,
        protected val queryTimeout: Int = 2,
        protected val rollbackOnBatchFailure: Boolean = true
) : LazyBasicBolt<T>(
        srcPos,
        srcField
)