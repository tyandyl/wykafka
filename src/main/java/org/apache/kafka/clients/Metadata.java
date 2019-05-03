
package org.apache.kafka.clients;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.InvalidMetadataException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.LogContext;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Metadata中的字段可以由主线程读、Sender线程更新，,所以方法必须加锁
 * 这个类和cluster的关系在MetadataCache里边维护
 */
public class Metadata implements Closeable {
    // 两次发出更新Cluster保存的元数据信息的最小时问差，默认为100ms，metadata 更新失败时,为避免频繁更新 meta,最小的间隔时间,默认 100ms
    private final long refreshBackoffMs;
    private final long metadataExpireMs;// 每隔多久，更新一次。默认是300 x 1000,也就是5分种
    private int updateVersion; // 每更新成功1次，version自增1,主要是用于判断 metadata 是否更新
    private int requestVersion; // 请求版本号
    private long lastRefreshMs;// 最近一次更新时的时间（包含更新失败的情况），记录上一次更新元数据的时间戳(也包含更新失败的情况)
    private long lastSuccessfulRefreshMs; // 最近一次成功更新的时间（如果每次都成功的话，与前面的值相等, 否则，lastSuccessulRefreshMs < lastRefreshMs)
    private AuthenticationException authenticationException;
    private KafkaException metadataException;
    private MetadataCache cache = MetadataCache.empty();
    private boolean needUpdate;// 是都需要更新 metadata，标识是否强制更新Cluster，这是触发Sender线程吏新集群儿数据的条件之一。
    //监听Metadata更新的监听器集合自定义Metadata监听实现Mctadata,Listener.onMetadataUpdate()方法即可，
    // 在更新Metadata } }，的cluster段之前，会通知listener集合中全部Listene:对象。
    private final ClusterResourceListeners clusterResourceListeners;
    private boolean isClosed;
    //TopicPartition表示某Topic的一个分区，其中的topic字段是Topic的名称，partition字段则此分区在Topic中的分区编号(ID )
    private final Map<TopicPartition, Integer> lastSeenLeaderEpochs;


    public Metadata(long refreshBackoffMs,
                    long metadataExpireMs,
                    LogContext logContext,
                    ClusterResourceListeners clusterResourceListeners) {
        this.refreshBackoffMs = refreshBackoffMs;
        this.metadataExpireMs = metadataExpireMs;
        this.lastRefreshMs = 0L;
        this.lastSuccessfulRefreshMs = 0L;
        this.requestVersion = 0;
        this.updateVersion = 0;
        this.needUpdate = false;
        this.clusterResourceListeners = clusterResourceListeners;
        this.isClosed = false;
        this.lastSeenLeaderEpochs = new HashMap<>();
    }

    /**
     * Get the current cluster info without blocking
     */
    public synchronized Cluster fetch() {
        return cache.cluster();
    }

    /**
     * Return the next time when the current cluster info can be updated (i.e., backoff time has elapsed).
     *
     * @param nowMs current time in ms
     * @return remaining time in ms till the cluster info can be updated again
     */
    public synchronized long timeToAllowUpdate(long nowMs) {
        return Math.max(this.lastRefreshMs + this.refreshBackoffMs - nowMs, 0);
    }

    /**
     * The next time to update the cluster info is the maximum of the time the current info will expire and the time the
     * current info can be updated (i.e. backoff time has elapsed); If an update has been request then the expiry time
     * is now
     *
     * @param nowMs current time in ms
     * @return remaining time in ms till updating the cluster info
     */
    public synchronized long timeToNextUpdate(long nowMs) {
        long timeToExpire = needUpdate ? 0 : Math.max(this.lastSuccessfulRefreshMs + this.metadataExpireMs - nowMs, 0);
        return Math.max(timeToExpire, timeToAllowUpdate(nowMs));
    }

    /**
     * 加锁，因为两个线程使用，
     * @return
     */
    public synchronized int requestUpdate() {
        //表示需要强制更新Cluster
        this.needUpdate = true;
        //返回当前元数据的版本号
        return this.updateVersion;
    }

    /**
     * Request an update for the partition metadata iff the given leader epoch is at newer than the last seen leader epoch
     */
    public synchronized boolean updateLastSeenEpochIfNewer(TopicPartition topicPartition, int leaderEpoch) {
        Objects.requireNonNull(topicPartition, "TopicPartition cannot be null");
        return updateLastSeenEpoch(topicPartition, leaderEpoch, oldEpoch -> leaderEpoch > oldEpoch, true);
    }


    public Optional<Integer> lastSeenLeaderEpoch(TopicPartition topicPartition) {
        return Optional.ofNullable(lastSeenLeaderEpochs.get(topicPartition));
    }

    /**
     * Conditionally update the leader epoch for a partition
     *
     * @param topicPartition       topic+partition to update the epoch for
     * @param epoch                the new epoch
     * @param epochTest            a predicate to determine if the old epoch should be replaced
     * @param setRequestUpdateFlag sets the "needUpdate" flag to true if the epoch is updated
     * @return true if the epoch was updated, false otherwise
     */
    private synchronized boolean updateLastSeenEpoch(TopicPartition topicPartition,
                                                     int epoch,
                                                     Predicate<Integer> epochTest,
                                                     boolean setRequestUpdateFlag) {
        Integer oldEpoch = lastSeenLeaderEpochs.get(topicPartition);
        if (oldEpoch == null || epochTest.test(oldEpoch)) {

            lastSeenLeaderEpochs.put(topicPartition, epoch);
            if (setRequestUpdateFlag) {
                this.needUpdate = true;
            }
            return true;
        } else {

            return false;
        }
    }

    /**
     * Check whether an update has been explicitly requested.
     *
     * @return true if an update was requested, false otherwise
     */
    public synchronized boolean updateRequested() {
        return this.needUpdate;
    }

    /**
     * Return the cached partition info if it exists and a newer leader epoch isn't known about.
     */
    public synchronized Optional<MetadataCache.PartitionInfoAndEpoch> partitionInfoIfCurrent(TopicPartition topicPartition) {
        Integer epoch = lastSeenLeaderEpochs.get(topicPartition);
        if (epoch == null) {
            // old cluster format (no epochs)
            return cache.getPartitionInfo(topicPartition);
        } else {
            return cache.getPartitionInfoHavingEpoch(topicPartition, epoch);
        }
    }

    /**
     * If any non-retriable authentication exceptions were encountered during
     * metadata update, clear and return the exception.
     */
    public synchronized AuthenticationException getAndClearAuthenticationException() {
        if (authenticationException != null) {
            AuthenticationException exception = authenticationException;
            authenticationException = null;
            return exception;
        } else
            return null;
    }

    synchronized KafkaException getAndClearMetadataException() {
        if (this.metadataException != null) {
            KafkaException metadataException = this.metadataException;
            this.metadataException = null;
            return metadataException;
        } else
            return null;
    }

    public synchronized void bootstrap(List<InetSocketAddress> addresses, long now) {
        this.needUpdate = true;
        this.lastRefreshMs = now;
        this.lastSuccessfulRefreshMs = now;
        this.updateVersion += 1;
        this.cache = MetadataCache.bootstrap(addresses);
    }

    /**
     * Update metadata assuming the current request version. This is mainly for convenience in testing.
     */
    public synchronized void update(MetadataResponse response, long now) {
        this.update(this.requestVersion, response, now);
    }

    /**
     * Updates the cluster metadata. If topic expiry is enabled, expiry time
     * is set for topics if required and expired topics are removed from the metadata.
     *
     * @param requestVersion The request version corresponding to the update response, as provided by
     *     {@link #newMetadataRequestAndVersion()}.
     * @param response metadata response received from the broker
     * @param now current time in milliseconds
     */
    public synchronized void update(int requestVersion, MetadataResponse response, long now) {
        Objects.requireNonNull(response, "Metadata response cannot be null");
        if (isClosed())
            throw new IllegalStateException("Update requested after metadata close");

        if (requestVersion == this.requestVersion)
            this.needUpdate = false;
        else
            requestUpdate();

        this.lastRefreshMs = now;
        this.lastSuccessfulRefreshMs = now;
        this.updateVersion += 1;

        String previousClusterId = cache.cluster().clusterResource().clusterId();

        this.cache = handleMetadataResponse(response, topic -> retainTopic(topic.topic(), topic.isInternal(), now));

        Cluster cluster = cache.cluster();
        maybeSetMetadataError(cluster);

        this.lastSeenLeaderEpochs.keySet().removeIf(tp -> !retainTopic(tp.topic(), false, now));

        String newClusterId = cache.cluster().clusterResource().clusterId();
        if (!Objects.equals(previousClusterId, newClusterId)) {

        }
        clusterResourceListeners.onUpdate(cache.cluster().clusterResource());


    }

    private void maybeSetMetadataError(Cluster cluster) {
        // if we encounter any invalid topics, cache the exception to later throw to the user
        metadataException = null;
        checkInvalidTopics(cluster);
        checkUnauthorizedTopics(cluster);
    }

    private void checkInvalidTopics(Cluster cluster) {
        if (!cluster.invalidTopics().isEmpty()) {

            metadataException = new InvalidTopicException(cluster.invalidTopics());
        }
    }

    private void checkUnauthorizedTopics(Cluster cluster) {
        if (!cluster.unauthorizedTopics().isEmpty()) {

            metadataException = new TopicAuthorizationException(new HashSet<>(cluster.unauthorizedTopics()));
        }
    }

    /**
     * Transform a MetadataResponse into a new MetadataCache instance.
     */
    private MetadataCache handleMetadataResponse(MetadataResponse metadataResponse,
                                                 Predicate<MetadataResponse.TopicMetadata> topicsToRetain) {
        Set<String> internalTopics = new HashSet<>();
        List<MetadataCache.PartitionInfoAndEpoch> partitions = new ArrayList<>();
        for (MetadataResponse.TopicMetadata metadata : metadataResponse.topicMetadata()) {
            if (!topicsToRetain.test(metadata))
                continue;

            if (metadata.error() == Errors.NONE) {
                if (metadata.isInternal())
                    internalTopics.add(metadata.topic());
                for (MetadataResponse.PartitionMetadata partitionMetadata : metadata.partitionMetadata()) {
                    updatePartitionInfo(metadata.topic(), partitionMetadata, partitionInfo -> {
                        int epoch = partitionMetadata.leaderEpoch().orElse(RecordBatch.NO_PARTITION_LEADER_EPOCH);
                        partitions.add(new MetadataCache.PartitionInfoAndEpoch(partitionInfo, epoch));
                    });

                    if (partitionMetadata.error().exception() instanceof InvalidMetadataException) {

                        requestUpdate();
                    }
                }
            } else if (metadata.error().exception() instanceof InvalidMetadataException) {

                requestUpdate();
            }
        }

        return new MetadataCache(metadataResponse.clusterId(), new ArrayList<>(metadataResponse.brokers()), partitions,
                metadataResponse.topicsByError(Errors.TOPIC_AUTHORIZATION_FAILED),
                metadataResponse.topicsByError(Errors.INVALID_TOPIC_EXCEPTION),
                internalTopics, metadataResponse.controller());
    }

    /**
     * Compute the correct PartitionInfo to cache for a topic+partition and pass to the given consumer.
     */
    private void updatePartitionInfo(String topic,
                                     MetadataResponse.PartitionMetadata partitionMetadata,
                                     Consumer<PartitionInfo> partitionInfoConsumer) {

        TopicPartition tp = new TopicPartition(topic, partitionMetadata.partition());
        if (partitionMetadata.leaderEpoch().isPresent()) {
            int newEpoch = partitionMetadata.leaderEpoch().get();
            // If the received leader epoch is at least the same as the previous one, update the metadata
            if (updateLastSeenEpoch(tp, newEpoch, oldEpoch -> newEpoch >= oldEpoch, false)) {
                partitionInfoConsumer.accept(MetadataResponse.partitionMetaToInfo(topic, partitionMetadata));
            } else {
                // Otherwise ignore the new metadata and use the previously cached info
                PartitionInfo previousInfo = cache.cluster().partition(tp);
                if (previousInfo != null) {
                    partitionInfoConsumer.accept(previousInfo);
                }
            }
        } else {
            // Old cluster format (no epochs)
            lastSeenLeaderEpochs.clear();
            partitionInfoConsumer.accept(MetadataResponse.partitionMetaToInfo(topic, partitionMetadata));
        }
    }

    public synchronized void maybeThrowException() {
        AuthenticationException authenticationException = getAndClearAuthenticationException();
        if (authenticationException != null)
            throw authenticationException;

        KafkaException metadataException = getAndClearMetadataException();
        if (metadataException != null)
            throw metadataException;
    }

    /**
     * Record an attempt to update the metadata that failed. We need to keep track of this
     * to avoid retrying immediately.
     */
    public synchronized void failedUpdate(long now, AuthenticationException authenticationException) {
        this.lastRefreshMs = now;
        this.authenticationException = authenticationException;
    }

    /**
     * @return The current metadata updateVersion
     */
    public synchronized int updateVersion() {
        return this.updateVersion;
    }

    /**
     * The last time metadata was successfully updated.
     */
    public synchronized long lastSuccessfulUpdate() {
        return this.lastSuccessfulRefreshMs;
    }

    /**
     * Close this metadata instance to indicate that metadata updates are no longer possible.
     */
    @Override
    public synchronized void close() {
        this.isClosed = true;
    }

    /**
     * Check if this metadata instance has been closed. See {@link #close()} for more information.
     *
     * @return True if this instance has been closed; false otherwise
     */
    public synchronized boolean isClosed() {
        return this.isClosed;
    }

    public synchronized void requestUpdateForNewTopics() {
        // lastRefreshMs:最近一次更新时的时间（包含更新失败的情况），记录上一次更新元数据的时间戳(也包含更新失败的情况)
        this.lastRefreshMs = 0;
        // 请求版本号，随着topic的注册而增加
        this.requestVersion++;
        requestUpdate();
    }

    public synchronized MetadataRequestAndVersion newMetadataRequestAndVersion() {
        return new MetadataRequestAndVersion(newMetadataRequestBuilder(), requestVersion);
    }

    protected MetadataRequest.Builder newMetadataRequestBuilder() {
        return MetadataRequest.Builder.allTopics();
    }

    protected boolean retainTopic(String topic, boolean isInternal, long nowMs) {
        return true;
    }

    public static class MetadataRequestAndVersion {
        public final MetadataRequest.Builder requestBuilder;
        public final int requestVersion;

        private MetadataRequestAndVersion(MetadataRequest.Builder requestBuilder,
                                          int requestVersion) {
            this.requestBuilder = requestBuilder;
            this.requestVersion = requestVersion;
        }
    }

}
