package com.presisco.lazystorm.topology

import com.presisco.lazystorm.bolt.*
import com.presisco.lazystorm.bolt.jdbc.*
import com.presisco.lazystorm.bolt.json.FormattedJson2ListBolt
import com.presisco.lazystorm.bolt.json.FormattedJson2MapBolt
import com.presisco.lazystorm.bolt.json.Json2ListBolt
import com.presisco.lazystorm.bolt.json.Json2MapBolt
import com.presisco.lazystorm.bolt.kafka.KafkaKeySwitchBolt
import com.presisco.lazystorm.bolt.kafka.LazyJsonMapper
import com.presisco.lazystorm.bolt.redis.JedisMapListToHashBolt
import com.presisco.lazystorm.connector.DataSourceLoader
import com.presisco.lazystorm.connector.JedisPoolLoader
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.storm.generated.StormTopology
import org.apache.storm.kafka.bolt.KafkaBolt
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector
import org.apache.storm.kafka.spout.KafkaSpout
import org.apache.storm.kafka.spout.KafkaSpoutConfig
import org.apache.storm.topology.*
import org.apache.storm.tuple.Fields
import java.util.*
import kotlin.collections.ArrayList
import kotlin.collections.HashMap

class LazyTopoBuilder {

    private val dataSourceConfig = HashMap<String, HashMap<String, String>>()
    private val dataSourceLoaders = HashMap<String, DataSourceLoader>()

    private val jedisConfig = HashMap<String, HashMap<String, String>>()
    private val jedisLoaders = HashMap<String, JedisPoolLoader>()

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

    private fun <K, V> Map<String, *>.getMap(key: String) = this.byType<Map<K, V>>(key)

    private fun <K, V> Map<String, *>.getHashMap(key: String) = this.byType<HashMap<K, V>>(key)

    private fun <E> Map<String, *>.getList(key: String) = this.byType<List<E>>(key)

    private fun <E> Map<String, *>.getArrayList(key: String) = this.byType<ArrayList<E>>(key)

    private fun <K, V> Map<String, V>.mapKeyToHashMap(keyMap: (key: String) -> K): HashMap<K, V> {
        val hashMap = hashMapOf<K, V>()
        this.forEach { key, value -> hashMap[keyMap(key)] = value }
        return hashMap
    }

    private fun <Old, New> Map<String, Old>.mapValueToHashMap(valueMap: (value: Old) -> New): HashMap<String, New> {
        val hashMap = hashMapOf<String, New>()
        this.forEach { key, value -> hashMap[key] = valueMap(value) }
        return hashMap
    }

    private fun <T> collectionToArrayList(collection: Collection<T>): ArrayList<T> {
        val arrayList = ArrayList<T>(collection.size)
        arrayList.addAll(collection)
        return arrayList
    }

    fun loadDataSource(configs: Map<String, Map<String, String>>) {
        configs.forEach { name, config ->
            dataSourceConfig[name] = HashMap()
            config.forEach { key, value ->
                dataSourceConfig[name]!![key] = value
            }
            dataSourceLoaders[name] = DataSourceLoader().setConfig(name, dataSourceConfig[name]!!) as DataSourceLoader
        }
    }

    fun getDataSourceLoader(name: String) = dataSourceLoaders[name]!!

    fun loadRedisConfig(configs: Map<String, Map<String, String>>) {
        configs.forEach { name, config ->
            jedisConfig[name] = HashMap()
            config.forEach { key, value ->
                jedisConfig[name]!![key] = value
            }
            jedisLoaders[name] = JedisPoolLoader().setConfig(name, jedisConfig[name]!!) as JedisPoolLoader
        }
    }

    fun getJedisPoolLoader(name: String) = jedisLoaders[name]!!

    fun createLazyBolt(name: String, config: Map<String, Any>): Any? {
        with(config) {
            val itemClass = getOrDefault("class", "unknown")
            val srcPos = if (config.containsKey("srcPos")) getInt("srcPos") else Constants.DATA_FIELD_POS
            val srcField = if (config.containsKey("srcField")) getString("srcField") else Constants.DATA_FIELD_NAME

            val bolt = when (itemClass) {
                /*             Edit              */
                "MapRenameBolt" -> MapRenameBolt(getHashMap("rename"))
                "MapStripBolt" -> MapStripBolt(getArrayList("strip"))
                /*             Json              */
                "Json2MapBolt" -> Json2MapBolt()
                "Json2ListBolt" -> Json2ListBolt()
                "FormattedJson2MapBolt" -> {
                    val formatDefRaw = config["format"] as Map<String, Collection<String>>
                    val converted = formatDefRaw.mapValueToHashMap { collectionToArrayList(it) }
                    FormattedJson2MapBolt(converted)
                }
                "FormattedJson2ListBolt" -> {
                    val formatDefRaw = config["format"] as Map<String, Collection<String>>
                    val converted = formatDefRaw.mapValueToHashMap { collectionToArrayList(it) }
                    FormattedJson2ListBolt(converted)
                }
                /*             Kafka             */
                "KafkaKeySwitchBolt" -> {
                    val keyType = getString("key_type")
                    val valueType = getString("value_type")
                    if (valueType !in setOf("string")) {
                        throw IllegalStateException("unsupported value type: $valueType")
                    }
                    val keyStreamMap = getMap<String, String>("key_stream_map")
                    val converted = when (keyType) {
                        "int" -> keyStreamMap.mapKeyToHashMap { Integer.parseInt(it) }
                        "short" -> keyStreamMap.mapKeyToHashMap { Integer.parseInt(it).toShort() }
                        "string" -> keyStreamMap.mapKeyToHashMap { it }
                        else -> throw IllegalStateException("unsupported key type: $keyType")
                    }
                    when (keyType) {
                        "int" -> object : KafkaKeySwitchBolt<Int, String>(converted as HashMap<Int, String>) {}
                        "short" -> object : KafkaKeySwitchBolt<Short, String>(converted as HashMap<Short, String>) {}
                        "string" -> object : KafkaKeySwitchBolt<String, String>(converted as HashMap<String, String>) {}
                        else -> throw IllegalStateException("unsupported key type: $keyType")
                    }
                }
                "LazyKafkaDumpBolt" -> {
                    val props = Properties()
                    props["bootstrap.servers"] = getString("brokers")
                    props["acks"] = "1"
                    props["key.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
                    props["value.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
                    val mapper = LazyJsonMapper()
                    KafkaBolt<String, String>()
                            .withProducerProperties(props)
                            .withTopicSelector(DefaultTopicSelector(getString("topic")))
                            .withTupleToKafkaMapper(mapper)
                }
                /*             JDBC              */
                "BatchMapInsertJdbcBolt" -> BatchMapInsertJdbcBolt()
                        .setDataSource(getDataSourceLoader(getString("data_source")))
                        .setTableName(getString("table"))
                        .setQueryTimeout(getInt("timeout"))
                        .setRollbackOnFailure(getBoolean("rollback"))
                        .setBatchSize(getInt("batch_size"))
                        .setAck(getBoolean("ack"))
                        .setTickIntervalSec(getInt("interval"))
                "BatchMapReplaceJdbcBolt" -> BatchMapReplaceJdbcBolt()
                        .setDataSource(getDataSourceLoader(getString("data_source")))
                        .setTableName(getString("table"))
                        .setQueryTimeout(getInt("timeout"))
                        .setRollbackOnFailure(getBoolean("rollback"))
                        .setBatchSize(getInt("batch_size"))
                        .setAck(getBoolean("ack"))
                        .setTickIntervalSec(getInt("interval"))
                "SimpleInsertBolt", "MapInsertJdbcBolt" -> SimpleInsertBolt()
                        .setEmitOnException(getBoolean("emit_on_failure"))
                        .setDataSource(getDataSourceLoader(getString("data_source")))
                        .setTableName(getString("table"))
                        .setQueryTimeout(getInt("timeout"))
                        .setRollbackOnFailure(getBoolean("rollback"))
                "SimpleReplaceBolt", "MapReplaceJdbcBolt" -> SimpleReplaceBolt()
                        .setEmitOnException(getBoolean("emit_on_failure"))
                        .setDataSource(getDataSourceLoader(getString("data_source")))
                        .setTableName(getString("table"))
                        .setQueryTimeout(getInt("timeout"))
                        .setRollbackOnFailure(getBoolean("rollback"))
                "OracleSeqTagBolt" -> OracleSeqTagBolt(getString("sequence"), getString("tag"))
                        .setEmitOnException(getBoolean("emit_on_failure"))
                        .setDataSource(getDataSourceLoader(getString("data_source")))
                        .setQueryTimeout(getInt("timeout"))
                        .setRollbackOnFailure(getBoolean("rollback"))
                /*         Redis        */
                "JedisMapListToHashBolt" -> JedisMapListToHashBolt(getString("key_field"))
                        .setDataKey(getString("key"))
                        .setJedisPoolLoader(getJedisPoolLoader(getString("redis")))
                /*         Debug        */
                "TupleConsoleDumpBolt" -> TupleConsoleDumpBolt()
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

                    val validateUpstreamName = fun(upstreamName: String) {
                        if (!topoConfig.containsKey(upstreamName))
                            throw IllegalStateException("undefined upstream: $upstreamName for bolt: $name")
                    }

                    when (type) {
                        "spout" -> {
                            val spout: IRichSpout = try {
                                createLazySpout(name, config) ?: createSpout(name, config)
                            } catch (e: IllegalStateException) {
                                throw IllegalStateException("config for spout: $name is wrong, ${e.message}")
                            }
                            setSpout(
                                    name,
                                    spout,
                                    getInt("parallelism")
                            )
                        }
                        "bolt" -> {
                            val bolt: Any = try {
                                createLazyBolt(name, config) ?: createBolt(name, config)
                            } catch (e: IllegalStateException) {
                                throw IllegalStateException("config for bolt: $name is wrong, ${e.message}")
                            }
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
                                is Map<*, *> -> upstream.forEach { (boltName, streamName) ->
                                    validateUpstreamName(boltName as String)
                                    setGrouping(declarer, grouping, boltName, streamName as String, groupingParams)
                                }
                                is Collection<*> -> upstream.forEach {
                                    validateUpstreamName(it as String)
                                    setGrouping(declarer, grouping, it, groupingParams)
                                }
                                is String -> {
                                    validateUpstreamName(upstream)
                                    setGrouping(declarer, grouping, upstream, groupingParams)
                                }
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