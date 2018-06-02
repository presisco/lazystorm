package com.presisco.lazystorm.bolt.jdbc

import org.apache.storm.topology.base.BaseBasicBolt
import javax.sql.DataSource

abstract class BaseJdbcBolt(
        protected val dataSource: DataSource,
        protected val tableName: String,
        protected val queryTimeout: Int = 2,
        protected val rollbackOnBatchFailure: Boolean = true
): BaseBasicBolt()