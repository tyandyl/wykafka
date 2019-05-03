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

import java.util.Map;

import org.apache.kafka.common.utils.Utils;

/**
 *
 * MetricName [name=io-wait-ratio, group=producer-metrics, description=The fraction of time the I/O thread spent waiting., tags={client-id=MsgProducer}]=org.apache.kafka.common.metrics.KafkaMetric@462ee862
 * MetricName [name=request-size-avg, group=producer-metrics, description=The average size of all requests in the window.., tags={client-id=MsgProducer}]=org.apache.kafka.common.metrics.KafkaMetric@48e90ed7
 * MetricName [name=produce-throttle-time-max, group=producer-metrics, description=The maximum throttle time in ms, tags={client-id=MsgProducer}]=org.apache.kafka.common.metrics.KafkaMetric@4cd68312
 * MetricName [name=connection-creation-rate, group=producer-metrics, description=New connections established per second in the window., tags={client-id=MsgProducer}]=org.apache.kafka.common.metrics.KafkaMetric@9d404ec
 * MetricName [name=record-size-avg, group=producer-metrics, description=The average record size, tags={client-id=MsgProducer}]=org.apache.kafka.common.metrics.KafkaMetric@3808904c
 * MetricName [name=connection-close-rate, group=producer-metrics, description=Connections closed per second in the window., tags={client-id=MsgProducer}]=org.apache.kafka.common.metrics.KafkaMetric@3897ae2c
 * MetricName [name=record-queue-time-avg, group=producer-metrics, description=The average time in ms record batches spent in the record accumulator., tags={client-id=MsgProducer}]=org.apache.kafka.common.metrics.KafkaMetric@40a1643f
 * MetricName [name=request-latency-max, group=producer-metrics, description=The maximum request latency in ms, tags={client-id=MsgProducer}]=org.apache.kafka.common.metrics.KafkaMetric@69fbfba3
 * MetricName [name=request-rate, group=producer-metrics, description=The average number of requests sent per second., tags={client-id=MsgProducer}]=org.apache.kafka.common.metrics.KafkaMetric@158d9c20
 * MetricName [name=record-send-rate, group=producer-metrics, description=The average number of records sent per second., tags={client-id=MsgProducer}]=org.apache.kafka.common.metrics.KafkaMetric@106eb673
 * MetricName [name=io-wait-time-ns-avg, group=producer-metrics, description=The average length of time the I/O thread spent waiting for a socket ready for reads or writes in nanoseconds., tags={client-id=MsgProducer}]=org.apache.kafka.common.metrics.KafkaMetric@5ba5a8dd
 * MetricName [name=connection-count, group=producer-metrics, description=The current number of active connections., tags={client-id=MsgProducer}]=org.apache.kafka.common.metrics.KafkaMetric@6db566e9
 * MetricName [name=outgoing-byte-rate, group=producer-metrics, description=The average number of outgoing bytes sent per second to all servers., tags={client-id=MsgProducer}]=org.apache.kafka.common.metrics.KafkaMetric@14a09be3
 * MetricName [name=select-rate, group=producer-metrics, description=Number of times the I/O layer checked for new I/O to perform per second, tags={client-id=MsgProducer}]=org.apache.kafka.common.metrics.KafkaMetric@71186a2a
 * MetricName [name=record-size-max, group=producer-metrics, description=The maximum record size, tags={client-id=MsgProducer}]=org.apache.kafka.common.metrics.KafkaMetric@78dc794
 * MetricName [name=buffer-exhausted-rate, group=producer-metrics, description=The average per-second number of record sends that are dropped due to buffer exhaustion, tags={client-id=MsgProducer}]=org.apache.kafka.common.metrics.KafkaMetric@41c96ba2
 * MetricName [name=compression-rate-avg, group=producer-metrics, description=The average compression rate of record batches., tags={client-id=MsgProducer}]=org.apache.kafka.common.metrics.KafkaMetric@51a68ffb
 * MetricName [name=io-ratio, group=producer-metrics, description=The fraction of time the I/O thread spent doing I/O, tags={client-id=MsgProducer}]=org.apache.kafka.common.metrics.KafkaMetric@635de6df
 * MetricName [name=count, group=kafka-metrics-count, description=total number of registered metrics, tags={client-id=MsgProducer}]=org.apache.kafka.common.metrics.KafkaMetric@a853c47
 * MetricName [name=request-latency-avg, group=producer-metrics, description=The average request latency in ms, tags={client-id=MsgProducer}]=org.apache.kafka.common.metrics.KafkaMetric@9bd2ce
 * MetricName [name=record-error-rate, group=producer-metrics, description=The average per-second number of record sends that resulted in errors, tags={client-id=MsgProducer}]=org.apache.kafka.common.metrics.KafkaMetric@65a74c71
 * MetricName [name=metadata-age, group=producer-metrics, description=The age in seconds of the current producer metadata being used., tags={client-id=MsgProducer}]=org.apache.kafka.common.metrics.KafkaMetric@69403504
 * MetricName [name=request-size-max, group=producer-metrics, description=The maximum size of any request sent in the window., tags={client-id=MsgProducer}]=org.apache.kafka.common.metrics.KafkaMetric@4a044b6f
 * MetricName [name=requests-in-flight, group=producer-metrics, description=The current number of in-flight requests awaiting a response., tags={client-id=MsgProducer}]=org.apache.kafka.common.metrics.KafkaMetric@1457fd43
 * MetricName [name=buffer-available-bytes, group=producer-metrics, description=The total amount of buffer memory that is not being used (either unallocated or in the free list)., tags={client-id=MsgProducer}]=org.apache.kafka.common.metrics.KafkaMetric@4bccef7c
 * MetricName [name=io-time-ns-avg, group=producer-metrics, description=The average length of time for I/O per select call in nanoseconds., tags={client-id=MsgProducer}]=org.apache.kafka.common.metrics.KafkaMetric@23cc0b14
 * MetricName [name=buffer-total-bytes, group=producer-metrics, description=The maximum amount of buffer memory the client can use (whether or not it is currently used)., tags={client-id=MsgProducer}]=org.apache.kafka.common.metrics.KafkaMetric@4b7b6331
 * MetricName [name=waiting-threads, group=producer-metrics, description=The number of user threads blocked waiting for buffer memory to enqueue their records, tags={client-id=MsgProducer}]=org.apache.kafka.common.metrics.KafkaMetric@60ed22f8
 * MetricName [name=records-per-request-avg, group=producer-metrics, description=The average number of records per request., tags={client-id=MsgProducer}]=org.apache.kafka.common.metrics.KafkaMetric@4cf68696
 * MetricName [name=record-queue-time-max, group=producer-metrics, description=The maximum time in ms record batches spent in the record accumulator., tags={client-id=MsgProducer}]=org.apache.kafka.common.metrics.KafkaMetric@f21b0b7
 * MetricName [name=batch-size-avg, group=producer-metrics, description=The average number of bytes sent per partition per-request., tags={client-id=MsgProducer}]=org.apache.kafka.common.metrics.KafkaMetric@1f533889
 * MetricName [name=response-rate, group=producer-metrics, description=Responses received sent per second., tags={client-id=MsgProducer}]=org.apache.kafka.common.metrics.KafkaMetric@16202379
 * MetricName [name=bufferpool-wait-ratio, group=producer-metrics, description=The fraction of time an appender waits for space allocation., tags={client-id=MsgProducer}]=org.apache.kafka.common.metrics.KafkaMetric@6e1ac51d
 * MetricName [name=network-io-rate, group=producer-metrics, description=The average number of network operations (reads or writes) on all connections per second., tags={client-id=MsgProducer}]=org.apache.kafka.common.metrics.KafkaMetric@6cbc6461
 * MetricName [name=incoming-byte-rate, group=producer-metrics, description=Bytes/second read off all sockets, tags={client-id=MsgProducer}]=org.apache.kafka.common.metrics.KafkaMetric@6aad8e0f
 * MetricName [name=batch-size-max, group=producer-metrics, description=The max number of bytes sent per partition per-request., tags={client-id=MsgProducer}]=org.apache.kafka.common.metrics.KafkaMetric@7d0143c8
 * MetricName [name=record-retry-rate, group=producer-metrics, description=The average per-second number of retried record sends, tags={client-id=MsgProducer}]=org.apache.kafka.common.metrics.KafkaMetric@458b5358
 * MetricName [name=produce-throttle-time-avg, group=producer-metrics, description=The average throttle time in ms, tags={client-id=MsgProducer}]=org.apache.kafka.common.metrics.KafkaMetric@4a9bb8e4
 */
public final class MetricName {

    private final String name;//度量标准的名称
    private final String group;// 此度量标准所属的度量标准的组逻辑组名称
    private final String description;//包含在度量标准中的人类可读描述。 这是可选的。
    //度量标准的其他键/值属性。 这是可选的，搜索小伙子，藏得挺深的
    private Map<String, String> tags;//这个属性也没有方法，根据上边的注释，初步怀疑这个是标记度量的归属，标签
    private int hash = 0;

    //group，tags参数可用于在JMX或任何自定义报告中报告时创建唯一的度量标准名称。
    //例如：标准JMX MBean可以像domainName一样构造：type = group，key1 = val1，key2 = val2

    //用法看起来像这样：
    // set up metrics:
    // Map<String, String> metricTags = new LinkedHashMap<String, String>();
    // metricTags.put("client-id", "producer-1");
    // metricTags.put("topic", "topic");
    // MetricConfig metricConfig = new MetricConfig().tags(metricTags);

    public MetricName(String name, String group, String description, Map<String, String> tags) {
        this.name = Utils.notNull(name);
        this.group = Utils.notNull(group);
        this.description = Utils.notNull(description);
        this.tags = Utils.notNull(tags);
    }

    public String name() {
        return this.name;
    }

    public String group() {
        return this.group;
    }
    //小伙子，藏得挺深的
    public Map<String, String> tags() {
        return this.tags;
    }

    public String description() {
        return this.description;
    }

    @Override
    public int hashCode() {
        if (hash != 0)
            return hash;
        final int prime = 31;
        int result = 1;
        result = prime * result + group.hashCode();
        result = prime * result + name.hashCode();
        result = prime * result + tags.hashCode();
        this.hash = result;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        MetricName other = (MetricName) obj;
        return group.equals(other.group) && name.equals(other.name) && tags.equals(other.tags);
    }

    @Override
    public String toString() {
        return "MetricName [name=" + name + ", group=" + group + ", description="
                + description + ", tags=" + tags + "]";
    }
}
