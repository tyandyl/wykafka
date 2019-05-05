/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.producer;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.producer.internals.BufferPool;
import org.apache.kafka.clients.producer.internals.ProducerInterceptors;
import org.apache.kafka.clients.producer.internals.ProducerMetadata;
import org.apache.kafka.clients.producer.internals.ProducerMetrics;
import org.apache.kafka.clients.producer.internals.RecordAccumulator;
import org.apache.kafka.clients.producer.internals.Sender;
import org.apache.kafka.clients.producer.internals.TransactionManager;
import org.apache.kafka.clients.producer.internals.TransactionalRequestResult;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

/**
 * 消息的发送过程中，涉及到两个线程协调处理
 * 主线程首先将业务数据封装成ProducerRecord对象，之后调用send方法将消息放入RecordAccumulator（消息收集器）中暂存。
 * 我们也可以将消息收集器理解为主线程和sender线程之间的缓冲。
 * send线程负责将消息信息构成请求，并最终执行I/O操作，他从RecordAccumulator获取消息，并批量发送出去。
 *
 * 问题，为什么说，KafkaProducer是线程安全的？
 *
 * @param <K>
 * @param <V>
 */
public class KafkaProducer<K, V> implements Producer<K, V> {
    //clientId生存器，这里安全
    private static final AtomicInteger PRODUCER_CLIENT_ID_SEQUENCE = new AtomicInteger(1);
    private static final String JMX_PREFIX = "kafka.producer";
    public static final String NETWORK_THREAD_PREFIX = "kafka-producer-network-thread";
    public static final String PRODUCER_METRIC_GROUP_NAME = "producer-metrics";

    private final String clientId;//此生产的唯一标识，
    final Metrics metrics;
    private final Partitioner partitioner;//分区选择器，根据一定的策略，将消息路由到合适的分区。
    private final int maxRequestSize;//消息的最大长度，包括消息头，key和value
    private final long totalMemorySize;//发送单个消息的缓冲区大小 ？？？
    private final ProducerMetadata metadata;//整个Kafka集群的元数据。
    private final RecordAccumulator accumulator;//收集并缓存消息，等待Sender发送
    private final Sender sender;//发送消息的sender任务，实现Runnable接口，在ioThread线程中执行
    private final Thread ioThread;//执行sender任务发送消息的线程，称为：sender线程
    //压缩算法，可选项有none , gzip ,  snappy , 1z4。这是针对RecordAccumulato:中多条消息进行的压缩
    // ，所以消息越多，压缩效果越好。
    private final CompressionType compressionType;

    private final Sensor errors;
    private final Time time;//当前系统时间，也即生产者KafkaProducer客户端创建时间
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;//value的序列化器。
    private final ProducerConfig producerConfig;//配置对象，使用反射初始化KafkaProducer配置的相对对象
    private final long maxBlockTimeMs;//等待更新Kafka集群元数据的最大时长
    //拦截器，在消息发送之前对消息进行拦截，也可以先于用户的callBack,对ack响应进行处理
    private final ProducerInterceptors<K, V> interceptors;
    private final ApiVersions apiVersions;
    private final TransactionManager transactionManager;

    //初始化
    public KafkaProducer(final Map<String, Object> configs) {
        //配置，Key的序列化方法，value的序列化方法，元数据，客户端，拦截器，时间
        //Key的序列化方法，value的序列化方法，元数据，客户端，拦截器都可以为null
        this(configs, null, null, null, null, null, Time.SYSTEM);
    }

    public KafkaProducer(Map<String, Object> configs, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(configs, keySerializer, valueSerializer, null, null, null, Time.SYSTEM);
    }


    public KafkaProducer(Properties properties) {
        this(propsToMap(properties), null, null, null, null, null, Time.SYSTEM);
    }


    public KafkaProducer(Properties properties, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(propsToMap(properties), keySerializer, valueSerializer, null, null, null,
                Time.SYSTEM);
    }


    /**
     *
     * @param configs 配置
     * @param keySerializer Key的序列化方法
     * @param valueSerializer value的序列化方法
     * @param metadata 元数据
     * @param kafkaClient 客户端
     * @param interceptors 拦截器
     * @param time
     */
    KafkaProducer(Map<String, Object> configs,
                  Serializer<K> keySerializer,
                  Serializer<V> valueSerializer,
                  ProducerMetadata metadata,
                  KafkaClient kafkaClient,
                  ProducerInterceptors interceptors,
                  Time time) {
        //ProducerConfig.addSerializerToConfig 顾名思义，增加序列化到Config
        ProducerConfig config = new ProducerConfig(ProducerConfig.addSerializerToConfig(configs, keySerializer,
                valueSerializer));
        try {
            Map<String, Object> userProvidedConfigs = config.originals();
            this.producerConfig = config;
            //当前系统时间，也即生产者KafkaProducer客户端创建时间
            this.time = time;
            String clientId = config.getString("client.id");
            if (clientId.length() <= 0)
                clientId = "producer-" + PRODUCER_CLIENT_ID_SEQUENCE.getAndIncrement();
            this.clientId = clientId;
            //事务，重要，待看
            String transactionalId = userProvidedConfigs.containsKey("transactional.id") ?
                    (String) userProvidedConfigs.get("transactional.id") : null;
            //用来得到一个不变的映射，只映射指定键为指定的值,该度量标准的其他键/值属性。 这是可选的
            Map<String, String> metricTags = Collections.singletonMap("client-id", clientId);
            //Kafka运行状况度量信息，通过metrics.num.samples设置度量个数，默认值2，
            // 通过metrics.sample.window.ms设置度量信息有效时间，默认30000毫秒
            MetricConfig metricConfig = new MetricConfig().samples(config.getInt("metrics.num.samples"))
                    .timeWindow(config.getLong("metrics.sample.window.ms"), TimeUnit.MILLISECONDS)
                    .recordLevel(Sensor.RecordingLevel.forName(config.getString("metrics.recording.level")))
                    .tags(metricTags);
            //添加JMX监控
            //类的列表，用于衡量指标。实现MetricReporter接口，将允许增加一些类，这些类在新的衡量指标产生时就会改变。
            // JmxReporter总会包含用于注册JMX统计
            List<MetricsReporter> reporters = config.getConfiguredInstances("metric.reporters",
                    MetricsReporter.class,
                    Collections.singletonMap( "client.id", clientId));
            //kafka生产端监控，这里有个坑，Metrics 根本没有里边根本没有调用init，因为传参new了一个list
            reporters.add(new JmxReporter("kafka.producer"));
            //Kafka运行状况度量信息，通过metrics.num.samples设置度量个数，默认值2，通过metrics.sample.window.ms设置度量信息有效时间，默认30000毫秒
            this.metrics = new Metrics(metricConfig, reporters, time);
            //我觉得拿的应该是这个DefaultPartitioner类
            this.partitioner = config.getConfiguredInstance("partitioner.class", Partitioner.class);
            // 两次发出更新Cluster保存的元数据信息的最小时问差，默认为100ms，metadata 更新失败时,为避免频繁更新 meta,最小的间隔时间,默认 100ms
            long retryBackoffMs = config.getLong("retry.backoff.ms");
            if (keySerializer == null) {
                this.keySerializer = config.getConfiguredInstance("key.serializer", Serializer.class);
                this.keySerializer.configure(config.originals(), true);
            } else {
                config.ignore("key.serializer");
                this.keySerializer = keySerializer;
            }
            if (valueSerializer == null) {
                this.valueSerializer = config.getConfiguredInstance("value.serializer", Serializer.class);
                this.valueSerializer.configure(config.originals(), false);
            } else {
                config.ignore("value.serializer");
                this.valueSerializer = valueSerializer;
            }

            // 加载拦截器并确保它们得到clientid
            userProvidedConfigs.put( "client.id", clientId);
            ProducerConfig configWithClientId = new ProducerConfig(userProvidedConfigs, false);
            List<ProducerInterceptor<K, V>> interceptorList = (List) configWithClientId.getConfiguredInstances(
                    "interceptor.classes", ProducerInterceptor.class);
            if (interceptors != null)
                this.interceptors = interceptors;
            else
                this.interceptors = new ProducerInterceptors<>(interceptorList);
            //我擦，为毛把reporters放进去了
            ClusterResourceListeners clusterResourceListeners = configureClusterResourceListeners(keySerializer,
                    valueSerializer, interceptorList, reporters);
            //每次写入请求的字节数最大值，通过max.request.size配置，默认为1048576字节，也即1MB
            this.maxRequestSize = config.getInt("max.request.size");
            //缓冲区总内存大小，通过buffer.memory配置，默认值33554432字节，也即32MB
            this.totalMemorySize = config.getLong("buffer.memory");
            this.compressionType = CompressionType.forName(config.getString("compression.type"));

            this.maxBlockTimeMs = config.getLong("max.block.ms");
            this.transactionManager = configureTransactionState(config, null, null);
            int deliveryTimeoutMs = configureDeliveryTimeout(config);
            //后边看
            this.apiVersions = new ApiVersions();

            this.accumulator = new RecordAccumulator(null,
                    config.getInt("batch.size"),
                    this.compressionType,
                    config.getInt("linger.ms"),
                    retryBackoffMs,
                    deliveryTimeoutMs,
                    metrics,
                    PRODUCER_METRIC_GROUP_NAME,
                    time,
                    apiVersions,
                    transactionManager,
                    new BufferPool(this.totalMemorySize, config.getInt("batch.size"), metrics, time, PRODUCER_METRIC_GROUP_NAME));

//bootstrap.servers:用于建立与kafka集群连接的host/port组。数据将会在所有servers上均衡加载，不管哪些server是指定用于bootstrapping。
// 这个列表仅仅影响初始化的hosts（用于发现全部的servers）。这个列表格式：host1:port1,host2:port2,…
// 因为这些server仅仅是用于初始化的连接，以发现集群所有成员关系（可能会动态的变化），这个列表不需要包含所有的servers（你可能想要不止一个server，尽管这样，
// 可能某个server宕机了）。如果没有server在这个列表出现，则发送数据会一直失败，直到列表可用。
            //他会把host1:port1,host2:port2,…封装成InetSocketAddress，查看如果第一个链接失败怎么办？
            //我们把解析出来的addresses放入node节点
            List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(
                    config.getList("bootstrap.servers"),
                    config.getString("client.dns.lookup"));
            if (metadata != null) {
                this.metadata = metadata;
            } else {
                //在KafkaProducer中，使用Node, TopicPartition, PartitionInfo这三个类封装了Kafka集群的相关元数据，
                this.metadata = new ProducerMetadata(retryBackoffMs,
                        config.getLong("metadata.max.age.ms"),
                        null,//log日志
                        clusterResourceListeners,
                        Time.SYSTEM);
                this.metadata.bootstrap(addresses, time.milliseconds());
            }
            this.errors = this.metrics.sensor("errors");
            this.sender = newSender(null, kafkaClient, this.metadata);
            String ioThreadName = NETWORK_THREAD_PREFIX + " | " + clientId;
            this.ioThread = new KafkaThread(ioThreadName, this.sender, true);
            this.ioThread.start();

            AppInfoParser.registerAppInfo("kafka.producer", clientId, metrics);
        } catch (Throwable t) {
            // call close methods if internal objects are already constructed this is to prevent resource leak. see KAFKA-2121
            close(Duration.ofMillis(0), true);
            // now propagate the exception
            throw new KafkaException("Failed to construct kafka producer", t);
        }
    }

    // visible for testing
    Sender newSender(LogContext logContext, KafkaClient kafkaClient, ProducerMetadata metadata) {
        int maxInflightRequests = configureInflightRequests(producerConfig, transactionManager != null);
        int requestTimeoutMs = producerConfig.getInt("request.timeout.ms");
        ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder(producerConfig, time);
        ProducerMetrics metricsRegistry = new ProducerMetrics(this.metrics);
        Sensor throttleTimeSensor = Sender.throttleTimeSensor(metricsRegistry.senderMetrics);
        KafkaClient client = kafkaClient != null ? kafkaClient : new NetworkClient(
                new Selector(producerConfig.getLong("connections.max.idle.ms"),
                        this.metrics, time, "producer", channelBuilder, logContext),
                metadata,
                clientId,
                maxInflightRequests,
                producerConfig.getLong(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG),
                producerConfig.getLong(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG),
                producerConfig.getInt(ProducerConfig.SEND_BUFFER_CONFIG),
                producerConfig.getInt(ProducerConfig.RECEIVE_BUFFER_CONFIG),
                requestTimeoutMs,
                ClientDnsLookup.forConfig(producerConfig.getString(ProducerConfig.CLIENT_DNS_LOOKUP_CONFIG)),
                time,
                true,
                apiVersions,
                throttleTimeSensor,
                logContext);
        int retries = configureRetries(producerConfig, transactionManager != null, null);
        short acks = configureAcks(producerConfig, transactionManager != null, null);
        return new Sender(logContext,
                client,
                metadata,
                this.accumulator,
                maxInflightRequests == 1,
                producerConfig.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG),
                acks,
                retries,
                metricsRegistry.senderMetrics,
                time,
                requestTimeoutMs,
                producerConfig.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG),
                this.transactionManager,
                apiVersions);
    }

    private static int configureDeliveryTimeout(ProducerConfig config) {
        int deliveryTimeoutMs = config.getInt(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG);
        int lingerMs = config.getInt(ProducerConfig.LINGER_MS_CONFIG);
        int requestTimeoutMs = config.getInt(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);

        if (deliveryTimeoutMs < Integer.MAX_VALUE && deliveryTimeoutMs < lingerMs + requestTimeoutMs) {
            if (config.originals().containsKey(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG)) {
                // throw an exception if the user explicitly set an inconsistent value
                throw new ConfigException(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG
                    + " should be equal to or larger than " + ProducerConfig.LINGER_MS_CONFIG
                    + " + " + ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);
            } else {
                // override deliveryTimeoutMs default value to lingerMs + requestTimeoutMs for backward compatibility
                deliveryTimeoutMs = lingerMs + requestTimeoutMs;

            }
        }
        return deliveryTimeoutMs;
    }

    private static TransactionManager configureTransactionState(ProducerConfig config, LogContext logContext, Logger log) {

        TransactionManager transactionManager = null;

        boolean userConfiguredIdempotence = false;
        if (config.originals().containsKey(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG))
            userConfiguredIdempotence = true;

        boolean userConfiguredTransactions = false;
        if (config.originals().containsKey(ProducerConfig.TRANSACTIONAL_ID_CONFIG))
            userConfiguredTransactions = true;

        boolean idempotenceEnabled = config.getBoolean(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG);

        if (!idempotenceEnabled && userConfiguredIdempotence && userConfiguredTransactions)
            throw new ConfigException("Cannot set a " + ProducerConfig.TRANSACTIONAL_ID_CONFIG + " without also enabling idempotence.");

        if (userConfiguredTransactions)
            idempotenceEnabled = true;

        if (idempotenceEnabled) {
            String transactionalId = config.getString(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
            int transactionTimeoutMs = config.getInt(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG);
            long retryBackoffMs = config.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG);
            transactionManager = new TransactionManager(logContext, transactionalId, transactionTimeoutMs, retryBackoffMs);
            if (transactionManager.isTransactional())
                log.info("Instantiated a transactional producer.");
            else
                log.info("Instantiated an idempotent producer.");
        }

        return transactionManager;
    }

    private static int configureRetries(ProducerConfig config, boolean idempotenceEnabled, Logger log) {
        boolean userConfiguredRetries = false;
        if (config.originals().containsKey(ProducerConfig.RETRIES_CONFIG)) {
            userConfiguredRetries = true;
        }
        if (idempotenceEnabled && !userConfiguredRetries) {

            return Integer.MAX_VALUE;
        }
        if (idempotenceEnabled && config.getInt(ProducerConfig.RETRIES_CONFIG) == 0) {
            throw new ConfigException("Must set " + ProducerConfig.RETRIES_CONFIG + " to non-zero when using the idempotent producer.");
        }
        return config.getInt(ProducerConfig.RETRIES_CONFIG);
    }

    private static int configureInflightRequests(ProducerConfig config, boolean idempotenceEnabled) {
        if (idempotenceEnabled && 5 < config.getInt("max.in.flight.requests.per.connection")) {
            throw new ConfigException("Must set " + ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION + " to at most 5" +
                    " to use the idempotent producer.");
        }
        return config.getInt("max.in.flight.requests.per.connection");
    }

    private static short configureAcks(ProducerConfig config, boolean idempotenceEnabled, Logger log) {
        boolean userConfiguredAcks = false;
        short acks = (short) parseAcks(config.getString(ProducerConfig.ACKS_CONFIG));
        if (config.originals().containsKey(ProducerConfig.ACKS_CONFIG)) {
            userConfiguredAcks = true;
        }

        if (idempotenceEnabled && !userConfiguredAcks) {

            return -1;
        }

        if (idempotenceEnabled && acks != -1) {
            throw new ConfigException("Must set " + ProducerConfig.ACKS_CONFIG + " to all in order to use the idempotent " +
                    "producer. Otherwise we cannot guarantee idempotence.");
        }
        return acks;
    }

    private static int parseAcks(String acksString) {
        try {
            return acksString.trim().equalsIgnoreCase("all") ? -1 : Integer.parseInt(acksString.trim());
        } catch (NumberFormatException e) {
            throw new ConfigException("Invalid configuration value for 'acks': " + acksString);
        }
    }

    /**
     * Needs to be called before any other methods when the transactional.id is set in the configuration.
     *
     * This method does the following:
     *   1. Ensures any transactions initiated by previous instances of the producer with the same
     *      transactional.id are completed. If the previous instance had failed with a transaction in
     *      progress, it will be aborted. If the last transaction had begun completion,
     *      but not yet finished, this method awaits its completion.
     *   2. Gets the internal producer id and epoch, used in all future transactional
     *      messages issued by the producer.
     *
     * Note that this method will raise {@link TimeoutException} if the transactional state cannot
     * be initialized before expiration of {@code max.block.ms}. Additionally, it will raise {@link InterruptException}
     * if interrupted. It is safe to retry in either case, but once the transactional state has been successfully
     * initialized, this method should no longer be used.
     *
     * @throws IllegalStateException if no transactional.id has been configured
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
     *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
     * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
     *         transactional.id is not authorized. See the exception for more details
     * @throws KafkaException if the producer has encountered a previous fatal error or for any other unexpected error
     * @throws TimeoutException if the time taken for initialize the transaction has surpassed <code>max.block.ms</code>.
     * @throws InterruptException if the thread is interrupted while blocked
     */
    public void initTransactions() {
        throwIfNoTransactionManager();
        TransactionalRequestResult result = transactionManager.initializeTransactions();
        sender.wakeup();
        result.await(maxBlockTimeMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Should be called before the start of each new transaction. Note that prior to the first invocation
     * of this method, you must invoke {@link #initTransactions()} exactly one time.
     *
     * @throws IllegalStateException if no transactional.id has been configured or if {@link #initTransactions()}
     *         has not yet been invoked
     * @throws ProducerFencedException if another producer with the same transactional.id is active
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
     *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
     * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
     *         transactional.id is not authorized. See the exception for more details
     * @throws KafkaException if the producer has encountered a previous fatal error or for any other unexpected error
     */
    public void beginTransaction() throws ProducerFencedException {
        throwIfNoTransactionManager();
        transactionManager.beginTransaction();
    }

    /**
     * Sends a list of specified offsets to the consumer group coordinator, and also marks
     * those offsets as part of the current transaction. These offsets will be considered
     * committed only if the transaction is committed successfully. The committed offset should
     * be the next message your application will consume, i.e. lastProcessedMessageOffset + 1.
     * <p>
     * This method should be used when you need to batch consumed and produced messages
     * together, typically in a consume-transform-produce pattern. Thus, the specified
     * {@code consumerGroupId} should be the same as config parameter {@code group.id} of the used
     * {@link KafkaConsumer consumer}. Note, that the consumer should have {@code enable.auto.commit=false}
     * and should also not commit offsets manually (via {@link KafkaConsumer#commitSync(Map) sync} or
     * {@link KafkaConsumer#commitAsync(Map, OffsetCommitCallback) async} commits).
     *
     * @throws IllegalStateException if no transactional.id has been configured or no transaction has been started
     * @throws ProducerFencedException fatal error indicating another producer with the same transactional.id is active
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
     *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
     * @throws org.apache.kafka.common.errors.UnsupportedForMessageFormatException  fatal error indicating the message
     *         format used for the offsets topic on the broker does not support transactions
     * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
     *         transactional.id is not authorized. See the exception for more details
     * @throws KafkaException if the producer has encountered a previous fatal or abortable error, or for any
     *         other unexpected error
     */
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                         String consumerGroupId) throws ProducerFencedException {
        throwIfNoTransactionManager();
        TransactionalRequestResult result = transactionManager.sendOffsetsToTransaction(offsets, consumerGroupId);
        sender.wakeup();
        result.await();
    }

    public void commitTransaction() throws ProducerFencedException {
        throwIfNoTransactionManager();
        TransactionalRequestResult result = transactionManager.beginCommit();
        sender.wakeup();
        result.await(maxBlockTimeMs, TimeUnit.MILLISECONDS);
    }

    public void abortTransaction() throws ProducerFencedException {
        throwIfNoTransactionManager();
        TransactionalRequestResult result = transactionManager.beginAbort();
        sender.wakeup();
        result.await(maxBlockTimeMs, TimeUnit.MILLISECONDS);
    }


    /**
     * 发送消息，实际上是把消息放入RecordAccumulator中
     * @param record
     * @return
     */
    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        return send(record, null);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
        //拦截器
        ProducerRecord<K, V> interceptedRecord = this.interceptors.onSend(record);
        return doSend(interceptedRecord, callback);
    }

    // Verify that this producer instance has not been closed. This method throws IllegalStateException if the producer
    // has already been closed.
    private void throwIfProducerClosed() {
        if (ioThread == null || !ioThread.isAlive())
            throw new IllegalStateException("Cannot perform operation after producer has been closed");
    }


    private Future<RecordMetadata> doSend(ProducerRecord<K, V> record, Callback callback) {
        TopicPartition tp = null;
        try {
            //检查线程存活
            throwIfProducerClosed();

            ClusterAndWaitTime clusterAndWaitTime;
            try {
                //maxBlockTimeMs:等待更新Kafka集群元数据的最大时长
                clusterAndWaitTime = waitOnMetadata(record.topic(), record.partition(), maxBlockTimeMs);
            } catch (KafkaException e) {
                if (metadata.isClosed())
                    throw new KafkaException("Producer closed while send in progress", e);
                throw e;
            }
            //maxBlockTimeMs:等待更新Kafka集群元数据的最大时长
            long remainingWaitMs = Math.max(0, maxBlockTimeMs - clusterAndWaitTime.waitedOnMetadataMs);
            Cluster cluster = clusterAndWaitTime.cluster;
            byte[] serializedKey;
            try {
                serializedKey = keySerializer.serialize(record.topic(), record.headers(), record.key());
            } catch (ClassCastException cce) {
                throw new SerializationException("Can't convert key of class " + record.key().getClass().getName() +
                        " to class " + producerConfig.getClass(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG).getName() +
                        " specified in key.serializer", cce);
            }
            byte[] serializedValue;
            try {
                serializedValue = valueSerializer.serialize(record.topic(), record.headers(), record.value());
            } catch (ClassCastException cce) {
                throw new SerializationException("Can't convert value of class " + record.value().getClass().getName() +
                        " to class " + producerConfig.getClass(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG).getName() +
                        " specified in value.serializer", cce);
            }
            //路由分区在这里
            int partition = partition(record, serializedKey, serializedValue, cluster);
            tp = new TopicPartition(record.topic(), partition);

            setReadOnly(record.headers());
            Header[] headers = record.headers().toArray();
            //估算消息的大小
            //https://www.cnblogs.com/qwangxiao/p/9043491.html
            int serializedSize = AbstractRecords.estimateSizeInBytesUpperBound(apiVersions.maxUsableProduceMagic(),
                    compressionType, serializedKey, serializedValue, headers);
            //检查大小不超过maxRequestSize和totalMemorySize
            ensureValidRecordSize(serializedSize);
            //获取消息的时间戳
            //http://www.codeleading.com/article/1096593174/
            long timestamp = record.timestamp() == null ? time.milliseconds() : record.timestamp();

            //构建一个InterceptorCallback回调对象
            Callback interceptCallback = new InterceptorCallback<>(callback, this.interceptors, tp);

            if (transactionManager != null && transactionManager.isTransactional())
                transactionManager.maybeAddPartitionToTransaction(tp);
            //队列容器中执行添加数据操作（  重要,跟性能有关）,供Sender线程去读取数据，然后发给broker
            RecordAccumulator.RecordAppendResult result = accumulator.append(tp, timestamp, serializedKey,
                    serializedValue, headers, interceptCallback, remainingWaitMs);
            if (result.batchIsFull || result.newBatchCreated) {
                this.sender.wakeup();
            }
            return result.future;

        } catch (ApiException e) {
            if (callback != null)
                callback.onCompletion(null, e);
            this.errors.record();
            this.interceptors.onSendError(record, tp, e);
            return new FutureFailure(e);
        } catch (InterruptedException e) {
            this.errors.record();
            this.interceptors.onSendError(record, tp, e);
            throw new InterruptException(e);
        } catch (BufferExhaustedException e) {
            this.errors.record();
            this.metrics.sensor("buffer-exhausted-records").record();
            this.interceptors.onSendError(record, tp, e);
            throw e;
        } catch (KafkaException e) {
            this.errors.record();
            this.interceptors.onSendError(record, tp, e);
            throw e;
        } catch (Exception e) {
            // we notify interceptor about all exceptions, since onSend is called before anything else in this method
            this.interceptors.onSendError(record, tp, e);
            throw e;
        }
    }

    private void setReadOnly(Headers headers) {
        if (headers instanceof RecordHeaders) {
            ((RecordHeaders) headers).setReadOnly();
        }
    }

    /**
     *
     * @param topic
     * @param partition
     * @param maxWaitMs
     * @return
     * @throws InterruptedException
     */
    private ClusterAndWaitTime waitOnMetadata(String topic, Integer partition, long maxWaitMs) throws InterruptedException {
       //从元数据中获取Cluster，看看他的初始化
        Cluster cluster = metadata.fetch();

        if (cluster.invalidTopics().contains(topic))
            throw new InvalidTopicException(topic);
        //requestVersion会随着topic的注册而增加
        //终于明白这句话了：Kafka集群元数据每更新成功1次，version字段的值增1
        //这里的更新就是add吧
        metadata.add(topic);
        //返回当前topic的分区partition 个数。
        Integer partitionsCount = cluster.partitionCountForTopic(topic);
        //partition 为指定的要发往的分区，如果是null，或者分区id小于分区的数量
        if (partitionsCount != null && (partition == null || partition < partitionsCount))
            //这里的0是等待更新Kafka集群元数据的最大时长，为什么这里要设成0呢
            return new ClusterAndWaitTime(cluster, 0);

        long begin = time.milliseconds();
        long remainingWaitMs = maxWaitMs;//maxWaitMs是等待更新Kafka集群元数据的最大时长
        long elapsed;
        // Issue metadata requests until we have metadata for the topic and the requested partition,
        // or until maxWaitTimeMs is exceeded. This is necessary in case the metadata
        // is stale and the number of partitions for this topic has increased in the meantime.
        do {
            if (partition != null) {

            } else {
            }
            metadata.add(topic);
            int version = metadata.requestUpdate();
            // 将Sender线程从阻塞中唤醒
            //https://www.jianshu.com/p/a07a927d98ec
            sender.wakeup();
            try {
                metadata.awaitUpdate(version, remainingWaitMs);
            } catch (TimeoutException ex) {
                // Rethrow with original maxWaitMs to prevent logging exception with remainingWaitMs
                throw new TimeoutException(
                        String.format("Topic %s not present in metadata after %d ms.",
                                topic, maxWaitMs));
            }
            cluster = metadata.fetch();
            elapsed = time.milliseconds() - begin;
            //maxWaitMs是等待更新Kafka集群元数据的最大时长
            if (elapsed >= maxWaitMs) {
                throw new TimeoutException(partitionsCount == null ?
                        String.format("Topic %s not present in metadata after %d ms.",
                                topic, maxWaitMs) :
                        String.format("Partition %d of topic %s with partition count %d is not present in metadata after %d ms.",
                                partition, topic, partitionsCount, maxWaitMs));
            }
            metadata.maybeThrowException();
            remainingWaitMs = maxWaitMs - elapsed;
            partitionsCount = cluster.partitionCountForTopic(topic);
        } while (partitionsCount == null || (partition != null && partition >= partitionsCount));

        return new ClusterAndWaitTime(cluster, elapsed);
    }

    /**
     * Validate that the record size isn't too large
     */
    private void ensureValidRecordSize(int size) {
        if (size > this.maxRequestSize)
            throw new RecordTooLargeException("The message is " + size +
                    " bytes when serialized which is larger than the maximum request size you have configured with the " +
                    ProducerConfig.MAX_REQUEST_SIZE_CONFIG +
                    " configuration.");
        if (size > this.totalMemorySize)
            throw new RecordTooLargeException("The message is " + size +
                    " bytes when serialized which is larger than the total memory buffer you have configured with the " +
                    ProducerConfig.BUFFER_MEMORY_CONFIG +
                    " configuration.");
    }

    /**
     *
     */
    @Override
    public void flush() {
        this.accumulator.beginFlush();
        this.sender.wakeup();
        try {
            this.accumulator.awaitFlushCompletion();
        } catch (InterruptedException e) {
            throw new InterruptException("Flush interrupted.", e);
        }
    }

    /**
     * Get the partition metadata for the given topic. This can be used for custom partitioning.
     * @throws AuthenticationException if authentication fails. See the exception for more details
     * @throws AuthorizationException if not authorized to the specified topic. See the exception for more details
     * @throws InterruptException if the thread is interrupted while blocked
     * @throws TimeoutException if metadata could not be refreshed within {@code max.block.ms}
     * @throws KafkaException for all Kafka-related exceptions, including the case where this method is called after producer close
     */
    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        Objects.requireNonNull(topic, "topic cannot be null");
        try {
            return waitOnMetadata(topic, null, maxBlockTimeMs).cluster.partitionsForTopic(topic);
        } catch (InterruptedException e) {
            throw new InterruptException(e);
        }
    }

    /**
     * Get the full set of internal metrics maintained by the producer.
     */
    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return Collections.unmodifiableMap(this.metrics.metrics());
    }

    /**
     * Close this producer. This method blocks until all previously sent requests complete.
     * This method is equivalent to <code>close(Long.MAX_VALUE, TimeUnit.MILLISECONDS)</code>.
     * <p>
     * <strong>If close() is called from {@link Callback}, a warning message will be logged and close(0, TimeUnit.MILLISECONDS)
     * will be called instead. We do this because the sender thread would otherwise try to join itself and
     * block forever.</strong>
     * <p>
     *
     * @throws InterruptException If the thread is interrupted while blocked
     */
    @Override
    public void close() {
        close(Duration.ofMillis(Long.MAX_VALUE));
    }

    /**
     * This method waits up to <code>timeout</code> for the producer to complete the sending of all incomplete requests.
     * <p>
     * If the producer is unable to complete all requests before the timeout expires, this method will fail
     * any unsent and unacknowledged records immediately.
     * <p>
     * If invoked from within a {@link Callback} this method will not block and will be equivalent to
     * <code>close(Duration.ofMillis(0))</code>. This is done since no further sending will happen while
     * blocking the I/O thread of the producer.
     *
     * @param timeout The maximum time to wait for producer to complete any pending requests. The value should be
     *                non-negative. Specifying a timeout of zero means do not wait for pending send requests to complete.
     * @throws InterruptException If the thread is interrupted while blocked
     * @throws IllegalArgumentException If the <code>timeout</code> is negative.
     *
     */
    @Override
    public void close(Duration timeout) {
        close(timeout, false);
    }

    private void close(Duration timeout, boolean swallowException) {
        long timeoutMs = timeout.toMillis();
        if (timeoutMs < 0)
            throw new IllegalArgumentException("The timeout cannot be negative.");


        // this will keep track of the first encountered exception
        AtomicReference<Throwable> firstException = new AtomicReference<>();
        boolean invokedFromCallback = Thread.currentThread() == this.ioThread;
        if (timeoutMs > 0) {
            if (invokedFromCallback) {

            } else {
                // Try to close gracefully.
                if (this.sender != null)
                    this.sender.initiateClose();
                if (this.ioThread != null) {
                    try {
                        this.ioThread.join(timeoutMs);
                    } catch (InterruptedException t) {
                        firstException.compareAndSet(null, new InterruptException(t));

                    }
                }
            }
        }

        if (this.sender != null && this.ioThread != null && this.ioThread.isAlive()) {

            this.sender.forceClose();
            // Only join the sender thread when not calling from callback.
            if (!invokedFromCallback) {
                try {
                    this.ioThread.join();
                } catch (InterruptedException e) {
                    firstException.compareAndSet(null, new InterruptException(e));
                }
            }
        }

        ClientUtils.closeQuietly(interceptors, "producer interceptors", firstException);
        ClientUtils.closeQuietly(metrics, "producer metrics", firstException);
        ClientUtils.closeQuietly(keySerializer, "producer keySerializer", firstException);
        ClientUtils.closeQuietly(valueSerializer, "producer valueSerializer", firstException);
        ClientUtils.closeQuietly(partitioner, "producer partitioner", firstException);
        AppInfoParser.unregisterAppInfo(JMX_PREFIX, clientId, metrics);

        Throwable exception = firstException.get();
        if (exception != null && !swallowException) {
            if (exception instanceof InterruptException) {
                throw (InterruptException) exception;
            }
            throw new KafkaException("Failed to close kafka producer", exception);
        }
    }

    private static Map<String, Object> propsToMap(Properties properties) {
        Map<String, Object> map = new HashMap<>(properties.size());
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            if (entry.getKey() instanceof String) {
                String k = (String) entry.getKey();
                map.put(k, properties.get(k));
            } else {
                throw new ConfigException(entry.getKey().toString(), entry.getValue(), "Key must be a string.");
            }
        }
        return map;
    }

    private ClusterResourceListeners configureClusterResourceListeners(Serializer<K> keySerializer, Serializer<V> valueSerializer, List<?>... candidateLists) {
        ClusterResourceListeners clusterResourceListeners = new ClusterResourceListeners();
        for (List<?> candidateList: candidateLists)
            clusterResourceListeners.maybeAddAll(candidateList);

        clusterResourceListeners.maybeAdd(keySerializer);
        clusterResourceListeners.maybeAdd(valueSerializer);
        return clusterResourceListeners;
    }

    /**
     * 路由分区
     * @param record
     * @param serializedKey
     * @param serializedValue
     * @param cluster
     * @return
     */
    private int partition(ProducerRecord<K, V> record, byte[] serializedKey, byte[] serializedValue, Cluster cluster) {
        Integer partition = record.partition();
        return partition != null ?
                partition :
                //org.apache.kafka.clients.producer.internals.DefaultPartitioner
                partitioner.partition(
                        record.topic(), record.key(), serializedKey, record.value(), serializedValue, cluster);
    }

    private void throwIfNoTransactionManager() {
        if (transactionManager == null)
            throw new IllegalStateException("Cannot use transactional methods without enabling transactions " +
                    "by setting the " + ProducerConfig.TRANSACTIONAL_ID_CONFIG + " configuration property");
    }

    // Visible for testing
    String getClientId() {
        return clientId;
    }

    private static class ClusterAndWaitTime {
        final Cluster cluster;
        final long waitedOnMetadataMs;
        ClusterAndWaitTime(Cluster cluster, long waitedOnMetadataMs) {
            this.cluster = cluster;
            this.waitedOnMetadataMs = waitedOnMetadataMs;
        }
    }

    private static class FutureFailure implements Future<RecordMetadata> {

        private final ExecutionException exception;

        public FutureFailure(Exception exception) {
            this.exception = new ExecutionException(exception);
        }

        @Override
        public boolean cancel(boolean interrupt) {
            return false;
        }

        @Override
        public RecordMetadata get() throws ExecutionException {
            throw this.exception;
        }

        @Override
        public RecordMetadata get(long timeout, TimeUnit unit) throws ExecutionException {
            throw this.exception;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

    }

    /**
     * A callback called when producer request is complete. It in turn calls user-supplied callback (if given) and
     * notifies producer interceptors about the request completion.
     */
    private static class InterceptorCallback<K, V> implements Callback {
        private final Callback userCallback;
        private final ProducerInterceptors<K, V> interceptors;
        private final TopicPartition tp;

        private InterceptorCallback(Callback userCallback, ProducerInterceptors<K, V> interceptors, TopicPartition tp) {
            this.userCallback = userCallback;
            this.interceptors = interceptors;
            this.tp = tp;
        }

        public void onCompletion(RecordMetadata metadata, Exception exception) {
            metadata = metadata != null ? metadata : new RecordMetadata(tp, -1, -1, RecordBatch.NO_TIMESTAMP, Long.valueOf(-1L), -1, -1);
            this.interceptors.onAcknowledgement(metadata, exception);
            if (this.userCallback != null)
                this.userCallback.onCompletion(metadata, exception);
        }
    }
}
