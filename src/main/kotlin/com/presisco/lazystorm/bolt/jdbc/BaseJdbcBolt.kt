package com.presisco.lazystorm.bolt.jdbc

import com.presisco.lazystorm.bolt.LazyBasicBolt
import javax.sql.DataSource

abstract class BaseJdbcBolt<out T>(
        protected val dataSource: DataSource,
        protected val tableName: String,
        protected val queryTimeout: Int = 2,
        protected val rollbackOnBatchFailure: Boolean = true
) : LazyBasicBolt<T>()