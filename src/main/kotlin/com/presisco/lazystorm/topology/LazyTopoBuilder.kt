package com.presisco.lazystorm.topology

import com.presisco.lazystorm.bolt.Constants
import com.presisco.lazystorm.bolt.LazyBasicBolt
import com.presisco.lazystorm.bolt.LazyTickBolt
import com.presisco.lazystorm.bolt.MapRenameBolt
import com.presisco.lazystorm.bolt.jdbc.BatchMapInsertJdbcBolt
import com.presisco.lazystorm.bolt.jdbc.BatchMapReplaceJdbcBolt
import com.presisco.lazystorm.bolt.jdbc.MapInsertJdbcBolt
import com.presisco.lazystorm.bolt.jdbc.MapReplaceJdbcBolt
import com.presisco.lazystorm.bolt.json.Json2ListBolt
import com.presisco.lazystorm.bolt.json.Json2MapBolt
import com.presisco.lazystorm.datasouce.DataSourceManager
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.storm.generated.StormTopology
import org.apache.storm.kafka.spout.KafkaSpout
import org.apache.storm.kafka.spout.KafkaSpoutConfig
import org.apache.storm.topology.*
import org.apache.storm.tuple.Fields

class LazyTopoBuilder {

    private fun setGrouping(
            declarer: BoltDeclarer,
            grouping: String,
            boltName: String
    ) = setGrouping(
            declarer,
            grouping,
            boltName,
            listOf()
    )

    private fun setGrouping(
            declarer: BoltDeclarer,
            grouping: String,
            boltName: String,
            params: List<String>
    ) {
        //val paramList = if(params == null) mutableListOf() else params.toMutableList()
        val paramList = params.toMutableList()

        with(declarer) {
            when (grouping) {
                "fields" -> fieldsGrouping(boltName, Fields(paramList))
            //"all" -> allGrouping(boltName)
            //"global" -> globalGrouping(boltName)
                "none" -> noneGrouping(boltName)
            //"direct" -> directGrouping(boltName)
                "shuffle" -> shuffleGrouping(boltName)
                else -> throw NoSuchFieldException("not supported grouping: $grouping")
            }
        }
    }

    private fun setGrouping(
            declarer: BoltDeclarer,
            grouping: String,
            boltName: String,
            streamName: String
    ) = setGrouping(
            declarer,
            grouping,
            boltName,
            streamName,
            listOf()
    )

    private fun setGrouping(
            declarer: BoltDeclarer,
            grouping: String,
            boltName: String,
            streamName: String,
            params: List<String>
    ) {
        val paramList = params.toMutableList()

        with(declarer) {
            when (grouping) {
                "fields" -> fieldsGrouping(boltName, streamName, Fields(paramList))
            //"all" -> allGrouping(boltName, streamName)
            //"global" -> globalGrouping(boltName, streamName)
                "none" -> noneGrouping(boltName, streamName)
            //"direct" -> directGrouping(boltName, streamName)
                "shuffle" -> shuffleGrouping(boltName, streamName)
                else -> throw NoSuchFieldException("not supported grouping: $grouping")
            }
        }
    }

    private fun <T> Map<String, *>.byType(key: String): T = if (this.containsKey(key)) this[key] as T else throw IllegalStateException("$key not defined in config")

    private fun Map<String, *>.getInt(key: String) = this.byType<Number>(key).toInt()

    private fun Map<String, *>.getLong(key: String) = this.byType<Number>(key).toLong()

    private fun Map<String, *>.getString(key: String) = this.byType<String>(key)

    private fun Map<String, *>.getBoolean(key: String) = this.byType<Boolean>(key)

    private fun <K, V> Map<String, *>.getHashMap(key: String) = this.byType<HashMap<K, V>>(key)

    private fun <E> Map<String, *>.getList(key: String) = this.byType<List<E>>(key)

    fun loadDataSource(dataSourceConfig: Map<String, Map<String, String>>) {
        DataSourceManager.loadDataSourceConfig(dataSourceConfig)
    }

    fun createLazyBolt(name: String, config: Map<String, Any>): Any? {
        with(config) {
            val itemClass = getString("class")
            val srcPos = if (config.containsKey("srcPos")) getInt("srcPos") else Constants.DATA_FIELD_POS
            val srcField = if (config.containsKey("srcField")) getString("srcField") else Constants.DATA_FIELD_NAME
            val bolt = when (itemClass) {
            //"KafkaKeySwitchBolt" -> KafkaKeySwitchBolt<Int, String>(getHashMap("key_to_stream"))
                "MapRenameBolt" -> MapRenameBolt(getHashMap("rename_map"))
            /*             Json              */
                "Json2MapBolt" -> Json2MapBolt()
                "Json2ListBolt" -> Json2ListBolt()
            /*             JDBC              */
                "BatchMapInsertJdbcBolt" -> BatchMapInsertJdbcBolt()
                        .setDataSourceName(getString("data_source"))
                        .setTableName(getString("table"))
                        .setQueryTimeout(getInt("timeout"))
                        .setRollbackOnFailure(getBoolean("rollback"))
                        .setBatchSize(getInt("batch_size"))
                        .setAck(getBoolean("ack"))
                        .setTickIntervalSec(getInt("interval"))
                "BatchMapReplaceJdbcBolt" -> BatchMapReplaceJdbcBolt()
                        .setDataSourceName(getString("data_source"))
                        .setTableName(getString("table"))
                        .setQueryTimeout(getInt("timeout"))
                        .setRollbackOnFailure(getBoolean("rollback"))
                        .setBatchSize(getInt("batch_size"))
                        .setAck(getBoolean("ack"))
                        .setTickIntervalSec(getInt("interval"))
                "MapInsertJdbcBolt" -> MapInsertJdbcBolt()
                        .setDataSourceName(getString("data_source"))
                        .setTableName(getString("table"))
                        .setQueryTimeout(getInt("timeout"))
                        .setRollbackOnFailure(getBoolean("rollback"))
                "MapReplaceJdbcBolt" -> MapReplaceJdbcBolt()
                        .setDataSourceName(getString("data_source"))
                        .setTableName(getString("table"))
                        .setQueryTimeout(getInt("timeout"))
                        .setRollbackOnFailure(getBoolean("rollback"))
                else -> null
            }
            bolt ?: return null
            when (bolt) {
                is LazyBasicBolt<*> -> bolt.setSrcPos(srcPos).setSrcField(srcField)
                is LazyTickBolt<*> -> bolt.setSrcPos(srcPos).setSrcField(srcField)
            }
            return bolt
        }
    }

    fun createLazySpout(name: String, config: Map<String, Any>): IRichSpout? {
        with(config) {
            val itemClass = getString("class")
            return when (itemClass) {
                "KafkaSpout" -> KafkaSpout(
                        KafkaSpoutConfig.Builder<String, String>(
                                getString("brokers"),
                                getString("topic")
                        ).setProp(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, getString("key.deserializer"))
                                .setProp(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, getString("value.deserializer"))
                                .setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.UNCOMMITTED_EARLIEST)
                                .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE)
                                .build()
                )
                else -> null
            }
        }
    }

    fun buildTopology(
            topoConfig: Map<String, Map<String, Any>>,
            createSpout: (name: String, config: Map<String, Any>) -> IRichSpout,
            createBolt: (name: String, config: Map<String, Any>) -> Any
    ): StormTopology {
        val builder = TopologyBuilder()
        with(builder) {
            topoConfig.forEach { name, config ->
                with(config) {
                    val type = getString("type")
                    when (type) {
                        "spout" -> {
                            val spout = createLazySpout(name, config) ?: createSpout(name, config)
                            setSpout(
                                    name,
                                    spout,
                                    getInt("parallelism")
                            )
                        }
                        "bolt" -> {
                            val bolt = createLazyBolt(name, config) ?: createBolt(name, config)
                            val declarer = when (bolt) {
                                is IBasicBolt -> setBolt(name, bolt, config.getInt("parallelism"))
                                is IRichBolt -> setBolt(name, bolt, config.getInt("parallelism"))
                                is IStatefulBolt<*> -> setBolt(name, bolt, config.getInt("parallelism"))
                                is IWindowedBolt -> setBolt(name, bolt, config.getInt("parallelism"))
                                is IStatefulWindowedBolt<*> -> setBolt(name, bolt, config.getInt("parallelism"))
                                else -> throw IllegalStateException("unsupported bolt type: ${bolt::class.java.simpleName}")
                            }

                            val upstream = config["upstream"]
                                    ?: throw IllegalStateException("null upstream for bolt: $name")
                            val grouping = if (config.containsKey("grouping")) config.getString("grouping") else "shuffle"

                            val groupingParams = if (config.containsKey("group_params"))
                                getList<String>("group_params")
                            else
                                listOf()

                            when (upstream) {
                                is Map<*, *> -> upstream.map { (boltName, streamName) -> setGrouping(declarer, grouping, boltName as String, streamName as String, groupingParams) }
                                is List<*> -> upstream.map { setGrouping(declarer, grouping, it as String, groupingParams) }
                                is String -> setGrouping(declarer, grouping, upstream, groupingParams)
                                else -> throw java.lang.Exception("bad upstream definition! $upstream")
                            }
                        }
                        else -> throw IllegalStateException("unsupported type: $type")
                    }
                }
            }
        }
        return builder.createTopology()
    }

}