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
package org.apache.kafka.common;

import org.apache.kafka.common.utils.Utils;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 *元数据类
 * Node、TopicPartition、PartitionInfo
 * Cluster的所有字段都是private final修饰的，且只提供了查询方法，并未提供任何修改方法，
 * 这就保证了这四个类的对象都是不可变性对象，它们也就成为r线程安全的对象
 */
public final class Cluster {

    private final boolean isBootstrapConfigured;
    private final List<Node> nodes;//Kafka集群中节点信息列表
    private final Set<String> unauthorizedTopics;
    private final Set<String> invalidTopics;
    private final Set<String> internalTopics;
    private final Node controller;
    //记录了TopicPartition与PartitionInfo的映射关系,逻辑上的分区和该分区详细信息的对应关系
    private final Map<TopicPartition, PartitionInfo> partitionsByTopicPartition;
    //记录了Topic名称和PartitionInfo的映射关系，可以按照Topic名称查询其中全部分区的详细信息
    private final Map<String, List<PartitionInfo>> partitionsByTopic;
    //Topic与PariitionInfo的映射关系，这里的List<Partitionlnfo>中存放的分区必须是有Leade:副本的Partition ,
    //而partitionsByTopic中记录的分区则不一定有Leade:副本，因为某些中间状态，例如Leader副本宕机而触发的选举过程中，
    //分区不一定有Leade:副本  重要
    private final Map<String, List<PartitionInfo>> availablePartitionsByTopic;
    //记录了Node与PartitionInfo的映射关系，可以按照节点Id查询其分布的全部分区的详细信息
    private final Map<Integer, List<PartitionInfo>> partitionsByNode;
    private final Map<Integer, Node> nodesById;//BrokerId与Node节点之间对应关系，方便按照BrokerId进行索引
    private final ClusterResource clusterResource;


    public Cluster(String clusterId,
                   Collection<Node> nodes,
                   Collection<PartitionInfo> partitions,
                   Set<String> unauthorizedTopics,
                   Set<String> internalTopics) {
        this(clusterId, false, nodes, partitions, unauthorizedTopics, Collections.emptySet(), internalTopics, null);
    }

    /**
     * Create a new cluster with the given id, nodes and partitions
     * @param nodes The nodes in the cluster
     * @param partitions Information about a subset of the topic-partitions this cluster hosts
     */
    public Cluster(String clusterId,
                   Collection<Node> nodes,
                   Collection<PartitionInfo> partitions,
                   Set<String> unauthorizedTopics,
                   Set<String> internalTopics,
                   Node controller) {
        this(clusterId, false, nodes, partitions, unauthorizedTopics, Collections.emptySet(), internalTopics, controller);
    }

    /**
     * Create a new cluster with the given id, nodes and partitions
     * @param nodes The nodes in the cluster
     * @param partitions Information about a subset of the topic-partitions this cluster hosts
     */
    public Cluster(String clusterId,
                   Collection<Node> nodes,
                   Collection<PartitionInfo> partitions,
                   Set<String> unauthorizedTopics,
                   Set<String> invalidTopics,
                   Set<String> internalTopics,
                   Node controller) {
        this(clusterId, false, nodes, partitions, unauthorizedTopics, invalidTopics, internalTopics, controller);
    }

    private Cluster(String clusterId,
                    boolean isBootstrapConfigured,
                    Collection<Node> nodes,
                    Collection<PartitionInfo> partitions,
                    Set<String> unauthorizedTopics,
                    Set<String> invalidTopics,
                    Set<String> internalTopics,
                    Node controller) {
        this.isBootstrapConfigured = isBootstrapConfigured;
        this.clusterResource = new ClusterResource(clusterId);

        List<Node> copy = new ArrayList<>(nodes);
        Collections.shuffle(copy);
        this.nodes = Collections.unmodifiableList(copy);

        // Index the nodes for quick lookup
        Map<Integer, Node> tmpNodesById = new HashMap<>();
        for (Node node : nodes)
            tmpNodesById.put(node.id(), node);
        this.nodesById = Collections.unmodifiableMap(tmpNodesById);

        // index the partition infos by topic, topic+partition, and node
        Map<TopicPartition, PartitionInfo> tmpPartitionsByTopicPartition = new HashMap<>(partitions.size());
        Map<String, List<PartitionInfo>> tmpPartitionsByTopic = new HashMap<>();
        Map<String, List<PartitionInfo>> tmpAvailablePartitionsByTopic = new HashMap<>();
        Map<Integer, List<PartitionInfo>> tmpPartitionsByNode = new HashMap<>();
        for (PartitionInfo p : partitions) {
            tmpPartitionsByTopicPartition.put(new TopicPartition(p.topic(), p.partition()), p);
            tmpPartitionsByTopic.merge(p.topic(), Collections.singletonList(p), Utils::concatListsUnmodifiable);
            if (p.leader() != null) {
                tmpAvailablePartitionsByTopic.merge(p.topic(), Collections.singletonList(p), Utils::concatListsUnmodifiable);
                tmpPartitionsByNode.merge(p.leader().id(), Collections.singletonList(p), Utils::concatListsUnmodifiable);
            }
        }
        this.partitionsByTopicPartition = Collections.unmodifiableMap(tmpPartitionsByTopicPartition);
        this.partitionsByTopic = Collections.unmodifiableMap(tmpPartitionsByTopic);
        this.availablePartitionsByTopic = Collections.unmodifiableMap(tmpAvailablePartitionsByTopic);
        this.partitionsByNode = Collections.unmodifiableMap(tmpPartitionsByNode);

        this.unauthorizedTopics = Collections.unmodifiableSet(unauthorizedTopics);
        this.invalidTopics = Collections.unmodifiableSet(invalidTopics);
        this.internalTopics = Collections.unmodifiableSet(internalTopics);
        this.controller = controller;
    }

    /**
     * Create an empty cluster instance with no nodes and no topic-partitions.
     */
    public static Cluster empty() {
        return new Cluster(null, new ArrayList<>(0), new ArrayList<>(0), Collections.emptySet(),
            Collections.emptySet(), null);
    }

    /**
     * Create a "bootstrap" cluster using the given list of host/ports
     * @param addresses The addresses
     * @return A cluster for these hosts/ports
     */
    public static Cluster bootstrap(List<InetSocketAddress> addresses) {
        List<Node> nodes = new ArrayList<>();
        int nodeId = -1;
        for (InetSocketAddress address : addresses)
            nodes.add(new Node(nodeId--, address.getHostString(), address.getPort()));
        return new Cluster(null, true, nodes, new ArrayList<>(0),
            Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), null);
    }

    /**
     * Return a copy of this cluster combined with `partitions`.
     */
    public Cluster withPartitions(Map<TopicPartition, PartitionInfo> partitions) {
        Map<TopicPartition, PartitionInfo> combinedPartitions = new HashMap<>(this.partitionsByTopicPartition);
        combinedPartitions.putAll(partitions);
        return new Cluster(clusterResource.clusterId(), this.nodes, combinedPartitions.values(),
                new HashSet<>(this.unauthorizedTopics), new HashSet<>(this.invalidTopics),
                new HashSet<>(this.internalTopics), this.controller);
    }

    /**
     * @return The known set of nodes
     */
    public List<Node> nodes() {
        return this.nodes;
    }

    /**
     * Get the node by the node id (or null if no such node exists)
     * @param id The id of the node
     * @return The node, or null if no such node exists
     */
    public Node nodeById(int id) {
        return this.nodesById.get(id);
    }

    /**
     * Get the current leader for the given topic-partition
     * @param topicPartition The topic and partition we want to know the leader for
     * @return The node that is the leader for this topic-partition, or null if there is currently no leader
     */
    public Node leaderFor(TopicPartition topicPartition) {
        PartitionInfo info = partitionsByTopicPartition.get(topicPartition);
        if (info == null)
            return null;
        else
            return info.leader();
    }

    /**
     * Get the metadata for the specified partition
     * @param topicPartition The topic and partition to fetch info for
     * @return The metadata about the given topic and partition, or null if none is found
     */
    public PartitionInfo partition(TopicPartition topicPartition) {
        return partitionsByTopicPartition.get(topicPartition);
    }

    /**
     * Get the list of partitions for this topic
     * @param topic The topic name
     * @return A list of partitions
     */
    public List<PartitionInfo> partitionsForTopic(String topic) {
        //JDK8新增便利的Map默认值方法.
        //允许调用者在代码语句中规定获得在map中符合提供的键的值，否则在没有找到提供的键的匹配项的时候返回一个“默认值”。
        return partitionsByTopic.getOrDefault(topic, Collections.emptyList());
    }


    public Integer partitionCountForTopic(String topic) {
        List<PartitionInfo> partitions = this.partitionsByTopic.get(topic);
        return partitions == null ? null : partitions.size();
    }


    public List<PartitionInfo> availablePartitionsForTopic(String topic) {
        //返回默认值
        return availablePartitionsByTopic.getOrDefault(topic, Collections.emptyList());
    }

    /**
     * Get the list of partitions whose leader is this node
     * @param nodeId The node id
     * @return A list of partitions
     */
    public List<PartitionInfo> partitionsForNode(int nodeId) {
        return partitionsByNode.getOrDefault(nodeId, Collections.emptyList());
    }

    /**
     * Get all topics.
     * @return a set of all topics
     */
    public Set<String> topics() {
        return partitionsByTopic.keySet();
    }

    public Set<String> unauthorizedTopics() {
        return unauthorizedTopics;
    }

    public Set<String> invalidTopics() {
        return invalidTopics;
    }

    public Set<String> internalTopics() {
        return internalTopics;
    }

    public boolean isBootstrapConfigured() {
        return isBootstrapConfigured;
    }

    public ClusterResource clusterResource() {
        return clusterResource;
    }

    public Node controller() {
        return controller;
    }

    @Override
    public String toString() {
        return "Cluster(id = " + clusterResource.clusterId() + ", nodes = " + this.nodes +
            ", partitions = " + this.partitionsByTopicPartition.values() + ", controller = " + controller + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Cluster cluster = (Cluster) o;
        return isBootstrapConfigured == cluster.isBootstrapConfigured &&
                Objects.equals(nodes, cluster.nodes) &&
                Objects.equals(unauthorizedTopics, cluster.unauthorizedTopics) &&
                Objects.equals(invalidTopics, cluster.invalidTopics) &&
                Objects.equals(internalTopics, cluster.internalTopics) &&
                Objects.equals(controller, cluster.controller) &&
                Objects.equals(partitionsByTopicPartition, cluster.partitionsByTopicPartition) &&
                Objects.equals(clusterResource, cluster.clusterResource);
    }

    @Override
    public int hashCode() {
        return Objects.hash(isBootstrapConfigured, nodes, unauthorizedTopics, invalidTopics, internalTopics, controller,
                partitionsByTopicPartition, clusterResource);
    }
}
