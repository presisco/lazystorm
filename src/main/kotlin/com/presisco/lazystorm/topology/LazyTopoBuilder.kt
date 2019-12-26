package com.presisco.lazystorm.topology

import com.presisco.lazystorm.*
import com.presisco.lazystorm.bolt.*
import com.presisco.lazystorm.bolt.jdbc.*
import com.presisco.lazystorm.bolt.json.FormattedJson2ListBolt
import com.presisco.lazystorm.bolt.json.FormattedJson2MapBolt
import com.presisco.lazystorm.bolt.json.Json2ListBolt
import com.presisco.lazystorm.bolt.json.Json2MapBolt
import com.presisco.lazystorm.bolt.kafka.KafkaKeySwitchBolt
import com.presisco.lazystorm.bolt.kafka.LazyJsonMapper
import com.presisco.lazystorm.bolt.redis.JedisMapListToHashBolt
import com.presisco.lazystorm.bolt.redis.JedisMapToHashBolt
import com.presisco.lazystorm.bolt.redis.JedisSingletonBolt
import com.presisco.lazystorm.connector.DataSourceLoader
import com.presisco.lazystorm.connector.JedisPoolLoader
import com.presisco.lazystorm.connector.LoaderManager
import com.presisco.lazystorm.connector.Neo4jLoader
import com.presisco.lazystorm.lifecycle.Configurable
import com.presisco.lazystorm.lifecycle.Connectable
import com.presisco.lazystorm.lifecycle.FlexStreams
import com.presisco.lazystorm.spout.TimedSpout
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.storm.generated.StormTopology
import org.apache.storm.kafka.bolt.KafkaBolt
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector
import org.apache.storm.kafka.spout.KafkaSpout
import org.apache.storm.kafka.spout.KafkaSpoutConfig
import org.apache.storm.topology.*
import org.apache.storm.topology.base.BaseWindowedBolt
import org.apache.storm.tuple.Fields
import org.apache.storm.utils.Utils
import java.util.*

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
                "fields" -> fieldsGrouping(boltName, Fields(*paramList.toTypedArray()))
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
                "fields" -> fieldsGrouping(boltName, streamName, Fields(*paramList.toTypedArray()))
                //"all" -> allGrouping(boltName, streamName)
                //"global" -> globalGrouping(boltName, streamName)
                "none" -> noneGrouping(boltName, streamName)
                //"direct" -> directGrouping(boltName, streamName)
                "shuffle" -> shuffleGrouping(boltName, streamName)
                else -> throw NoSuchFieldException("not supported grouping: $grouping")
            }
        }
    }

    fun assembleWindowBolt(bolt: LazyWindowedBolt<*>, name: String, config: Map<String, Any>) {
        when (config["window_mode"]) {
            "sliding_duration" -> bolt.withWindow(
                    BaseWindowedBolt.Duration.seconds(config.getInt("window_length")),
                    BaseWindowedBolt.Duration.seconds(config.getInt("sliding_interval"))
            )
            "tumbling_duration" -> bolt.withTumblingWindow(
                    BaseWindowedBolt.Duration.seconds(config.getInt("window_length"))
            )
            "sliding_count" -> bolt.withWindow(
                    BaseWindowedBolt.Count.of(config.getInt("window_length")),
                    BaseWindowedBolt.Count.of(config.getInt("sliding_interval"))
            )
            "tumbling_count" -> bolt.withTumblingWindow(
                    BaseWindowedBolt.Count.of(config.getInt("window_length"))
            )
        }
    }

    fun assembleConnectable(bolt: IComponent, name: String, config: Map<String, Any>) {
        with(config) {
            val connectable = bolt as Connectable<*>
            if (containsKey("data_source")) {
                (connectable as Connectable<DataSourceLoader>).connect(LoaderManager.getLoader("data_source", getString("data_source")))
            }
            if (containsKey("redis")) {
                (connectable as Connectable<JedisPoolLoader>).connect(LoaderManager.getLoader("redis", getString("redis")))
            }
            if (containsKey("neo4j")) {
                (connectable as Connectable<Neo4jLoader>).connect(LoaderManager.getLoader("neo4j", getString("neo4j")))
            }
            when (bolt) {
                is BaseJdbcBolt<*> -> {
                    bolt.setQueryTimeout(getInt("timeout"))
                            .setRollbackOnFailure(getBoolean("rollback"))
                    if (bolt is JdbcClientBolt<*>) {
                        bolt.setEmitOnException(getBoolean("emit_on_failure"))
                    }
                    val keyword = if (bolt is OracleSeqTagBolt) {
                        "sequence"
                    } else {
                        "table"
                    }

                    if (containsKey("stream_${keyword}_map")) {
                        bolt.setStreamTableMap(getHashMap("stream_${keyword}_map") as HashMap<String, String>)
                    } else if (containsKey(keyword)) {
                        bolt.setTableName(getString(keyword))
                    }
                }
                is JedisSingletonBolt<*> -> {
                    if (containsKey("stream_key_map")) {
                        bolt.setStreamKeyMap(getHashMap("stream_key_map") as HashMap<String, String>)
                    }
                    if (containsKey("key")) {
                        bolt.setDataKey(getString("key"))
                    }
                }
            }
            return
        }
    }

    fun createLazyBolt(name: String, config: Map<String, Any>, createCustomBolt: (name: String, config: Map<String, Any>) -> IComponent): IComponent {
        with(config) {
            val itemClass = getOrDefault("class", "unknown") as String
            val srcPos = if (config.containsKey("srcPos")) getInt("srcPos") else DATA_FIELD_POS
            val srcField = if (config.containsKey("srcField")) getString("srcField") else DATA_FIELD_NAME

            val bolt = when (itemClass) {
                /*             Edit              */
                "MapRenameBolt" -> MapRenameBolt(getHashMap("rename") as HashMap<String, String>)
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
                "SimpleInsertBolt", "MapInsertJdbcBolt" -> SimpleInsertBolt()
                "SimpleReplaceBolt", "MapReplaceJdbcBolt" -> SimpleReplaceBolt()
                "OracleSeqTagBolt" -> OracleSeqTagBolt(getString("tag"))
                "StreamFieldDirectInsertBolt" -> StreamFieldDirectInsertBolt(getString("field"))
                /*         Redis        */
                "JedisMapListToHashBolt" -> JedisMapListToHashBolt(getString("key_field"))
                "JedisMapToHashBolt" -> JedisMapToHashBolt()
                /*         Debug        */
                "TupleConsoleDumpBolt" -> TupleConsoleDumpBolt()
                else -> {
                    try {
                        val boltClass = Class.forName(itemClass)
                        boltClass.newInstance() as IComponent
                    } catch (e: ClassNotFoundException) {
                        createCustomBolt(name, config)
                    }
                }
            }
            when (bolt) {
                is LazyBasicBolt<*> -> bolt.setSrcPos(srcPos).setSrcField(srcField)
                is LazyTickBolt<*> -> bolt.setSrcPos(srcPos).setSrcField(srcField).setTickIntervalSec(config.getInt("tick_interval_sec"))
                is LazyWindowedBolt<*> -> assembleWindowBolt(bolt, name, config)
            }

            if (bolt is Connectable<*>) {
                assembleConnectable(bolt, name, config)
            }

            if (bolt is Configurable) {
                bolt.configure(config)
            }

            return bolt
        }
    }

    fun createLazySpout(name: String, config: Map<String, Any>, createSpout: (name: String, config: Map<String, Any>) -> IRichSpout): IRichSpout {
        with(config) {
            val itemClass = getString("class")
            val spout = when (itemClass) {
                "KafkaSpout" -> {
                    val spoutConfig = KafkaSpoutConfig.Builder<String, String>(
                            getString("brokers"),
                            getString("topic")
                    )
                            .setProp(ConsumerConfig.GROUP_ID_CONFIG, getString("group.id"))
                            .setProp(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, getString("key.deserializer"))
                            .setProp(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, getString("value.deserializer"))
                            .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE)
                    if (config.containsKey("group.id")) {
                        spoutConfig.setProp("group.id", config.getString("group.id"))
                    }
                    KafkaSpout(spoutConfig.build())
                }
                else -> {
                    if (itemClass.contains(".")) {
                        val spoutClass = Class.forName(itemClass)
                        spoutClass.newInstance() as IRichSpout
                    } else {
                        createSpout(name, config)
                    }
                }
            }
            when (spout) {
                is TimedSpout -> spout.setIntervalSec(getLong("interval"))
            }
            if (spout is Connectable<*>) {
                assembleConnectable(spout, name, config)
            }
            if (spout is Configurable) {
                spout.configure(config)
            }
            return spout
        }
    }

    fun declareBolt(builder: TopologyBuilder, bolt: Any, name: String, parallelism: Int) = with(builder) {
        when (bolt) {
            is IBasicBolt -> setBolt(name, bolt, parallelism)
            is IRichBolt -> setBolt(name, bolt, parallelism)
            is IStatefulBolt<*> -> setBolt(name, bolt, parallelism)
            is IWindowedBolt -> setBolt(name, bolt, parallelism)
            is IStatefulWindowedBolt<*> -> setBolt(name, bolt, parallelism)
            else -> throw IllegalStateException("unsupported bolt type: ${bolt::class.java.simpleName}")
        }
    }

    fun IComponent.scanOutputStreamNames(): Set<String> {
        val scanner = OutputScanner()
        this.declareOutputFields(scanner)
        return scanner.streams.keys
    }

    fun buildTopology(
            topoConfig: Map<String, Map<String, Any>>,
            createSpout: (name: String, config: Map<String, Any>) -> IRichSpout,
            createBolt: (name: String, config: Map<String, Any>) -> IComponent
    ): StormTopology {
        val builder = TopologyBuilder()
        with(builder) {
            val bolts = hashMapOf<String, IComponent>()
            topoConfig.forEach { name, config ->
                with(config) {
                    when (getString("type")) {
                        "spout" -> {
                            val spout: IRichSpout = try {
                                createLazySpout(name, config, createSpout)
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
                            val bolt = try {
                                createLazyBolt(name, config, createBolt)
                            } catch (e: IllegalStateException) {
                                throw IllegalStateException("config for bolt: $name is wrong, ${e.message}")
                            }

                            bolts[name] = bolt
                        }
                        else -> throw IllegalStateException("unsupported type: ${getString("type")}")
                    }
                }
            }

            // 统计每个Bolt依赖的上游Bolt
            val dependencyMap = hashMapOf<String, Collection<String>>()
            bolts.forEach { name, bolt ->
                val config = topoConfig[name] ?: error("mismatch bolt instances and topo config entries!")
                val upstream = config["upstream"] ?: throw IllegalStateException("null upstream for bolt: $name")
                dependencyMap[name] = when (upstream) {
                    is Map<*, *> -> upstream.keys as Set<String>
                    is List<*> -> upstream as List<String>
                    else -> setOf(upstream as String)
                }
            }

            // 根据依赖先后关系生成Bolt遍历顺序
            val boltOrder = LinkedList<String>()
            bolts.forEach { name, bolt ->
                var after = -1
                dependencyMap[name]!!.forEach {
                    val index = boltOrder.indexOf(it)
                    if (index > after) {
                        after = index
                    }
                }
                var before = bolts.size
                dependencyMap.forEach { downStream, dependencies ->
                    if (dependencies.contains(downStream)) {
                        val index = boltOrder.indexOf(downStream)
                        if (index > -1 && index < before) {
                            before = index
                        }
                    }
                }

                if (after == -1) {
                    boltOrder.addFirst(name)
                } else if (after > before) {
                    throw IllegalStateException("bad bolt dependency order at $name!")
                } else {
                    boltOrder.add(before, name)
                }

            }

            // 生成bolt分组关系
            boltOrder.forEach { name ->
                val bolt = bolts[name]!!
                val config = topoConfig[name] ?: error("mismatch bolt instances and topo config entries!")
                with(config) {
                    val declarer = declareBolt(builder, bolt, name, getInt("parallelism"))

                    val validateUpstreamName = fun(upstreamName: String) {
                        if (containsKey(upstreamName))
                            throw IllegalStateException("undefined upstream: $upstreamName for bolt: $name")
                    }

                    val upstream = config["upstream"]!!
                    val grouping = if (containsKey("grouping")) getString("grouping") else "shuffle"

                    val groupingParams = if (containsKey("group_params"))
                        getList<String>("group_params")
                    else
                        listOf()

                    val keepStream = if (bolt is FlexStreams && containsKey("keep_stream") && getBoolean("keep_stream")) {
                        if (upstream is Collection<*>) {
                            throw IllegalStateException("list upstream def for $name does not support keep_stream option")
                        }
                        true
                    } else {
                        false
                    }

                    try {
                        val inputStreams = hashSetOf<String>()
                        when (upstream) {
                            is Map<*, *> -> upstream.forEach { (boltName, streamDef) ->
                                validateUpstreamName(boltName as String)
                                val upstreams: Collection<String> = if (streamDef is String) {
                                    listOf(streamDef)
                                } else {
                                    streamDef as Collection<String>
                                }
                                upstreams.forEach { stream ->
                                    setGrouping(declarer, grouping, boltName, stream, groupingParams)
                                }
                                inputStreams.addAll(upstreams)
                            }
                            is Collection<*> -> upstream.forEach { boltName ->
                                validateUpstreamName(boltName as String)
                                val upstreamBolt = bolts[boltName]!!
                                if (keepStream) {
                                    val streams = if (upstreamBolt is FlexStreams) {
                                        upstreamBolt.getCustomStreams()
                                    } else {
                                        upstreamBolt.scanOutputStreamNames()
                                    }
                                    streams.forEach { setGrouping(declarer, grouping, boltName, it, groupingParams) }
                                    inputStreams.addAll(streams)
                                } else {
                                    setGrouping(declarer, grouping, boltName, groupingParams)
                                    inputStreams.add(Utils.DEFAULT_STREAM_ID)
                                }
                            }
                            else -> {
                                val upstreamBoltName = upstream as String
                                validateUpstreamName(upstreamBoltName)
                                if (keepStream) {
                                    val upstreamBolt = bolts[upstreamBoltName]!!
                                    val streams = if (upstreamBolt is FlexStreams) {
                                        upstreamBolt.getCustomStreams()
                                    } else {
                                        upstreamBolt.scanOutputStreamNames()
                                    }
                                    streams.forEach { setGrouping(declarer, grouping, upstreamBoltName, it, groupingParams) }
                                    inputStreams.addAll(streams)
                                } else {
                                    setGrouping(declarer, grouping, upstream, groupingParams)
                                    inputStreams.add(Utils.DEFAULT_STREAM_ID)
                                }
                            }
                        }
                        if (keepStream) {
                            (bolt as FlexStreams).addStreams(inputStreams.toList())
                        }
                    } catch (e: ClassCastException) {
                        throw Exception("bad upstream definition for bolt: $name! upstream in config: $upstream, supported types: String, List<String>, Map<String, String>, Map<String, List<String>>")
                    }
                }
            }
        }
        return builder.createTopology()
    }

}