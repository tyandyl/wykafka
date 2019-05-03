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
package org.apache.kafka.clients.producer.internals;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

public class DefaultPartitioner implements Partitioner {

    private final ConcurrentMap<String, AtomicInteger> topicCounterMap = new ConcurrentHashMap<>();

    public void configure(Map<String, ?> configs) {}

    //为消息选择分区编号
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        //获取主题的所有分区，用来实现消息的负载均衡
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        //若消息没有key，则均衡分布
        if (keyBytes == null) {
            //计数器递增,没有的话，先赋值一个，再递增
            int nextValue = nextValue(topic);
            //如果主副本挂了呢？会改变分区的的算法么
            List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic(topic);
            if (availablePartitions.size() > 0) {
                int part = Utils.toPositive(nextValue) % availablePartitions.size();
                return availablePartitions.get(part).partition();
            } else {
                // no partitions are available, give a non-available partition
                return Utils.toPositive(nextValue) % numPartitions;
            }
        } else {
            // 消息有key，则对消息的key进行散列化后取模
            return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
        }
    }

    private int nextValue(String topic) {
        AtomicInteger counter = topicCounterMap.get(topic);
        if (null == counter) {
            //ThreadLocalRandom类是JDK7在JUC包下新增的随机数生成器，
            // 它解决了Random类在多线程下多个线程竞争内部唯一的原子性种子变量而导致大量线程自旋重试的不足。
            //在JDK7之前包括现在java.util.Random应该是使用比较广泛的随机数生成工具类，
            // 另外java.lang.Math中的随机数生成也是使用的java.util.Random的实例。
            //例子：
            // Random random = new Random();
            //random.nextInt(5)
            //这里提下随机数的生成需要一个默认的种子，这个种子其实是一个long类型的数字,
            // 这个种子要么在Random的时候通过构造函数指定，要么默认构造函数内部会生成一个默认的值
            /**
             public int nextInt(int bound) {
             //(3)参数检查
             if (bound <= 0)
             throw new IllegalArgumentException(BadBound);
             //(4)根据老的种子生成新的种子
             int r = next(31);
             //(5)根据新的种子计算随机数
             ...
             return r;
             }
             如上代码可知新的随机数的生成需要两个步骤
             首先需要根据老的种子生成新的种子。
             然后根据新的种子来计算新的随机数。

             其中步骤（4）我们可以抽象为seed=f(seed),其中f是一个固定的函数，比如seed= f(seed)=a*seed+b;
             步骤（5）也可以抽象为g(seed,bound)，其中g是一个固定的函数，比如g(seed,bound)=(int)((bound * (long)seed) >> 31);
             在单线程情况下每次调用nextInt都是根据老的种子计算出来新的种子，这是可以保证随机数产生的随机性的。
             但是在多线程下多个线程可能都拿同一个老的种子去执行步骤（4）计算新的种子，这会导致多个线程产生的新种子是一样的，
             由于步骤（5）算法是固定的，所以会导致多个线程产生相同的随机值，这并不是我们想要的。
             所以步骤（4）要保证原子性，也就是说多个线程在根据同一个老种子计算新种子时候，第一个线程的新种子计算出来后，
             第二个线程要丢弃自己老的种子，要使用第一个线程的新种子来计算自己的新种子，依次类推，只有保证了这个，
             才能保证多线程下产生的随机数是随机的。Random函数使用一个原子变量达到了这个效果，
             在创建Random对象时候初始化的种子就保存到了种子原子变量里面，下面看下next()代码：
             protected int next(int bits) {
             long oldseed, nextseed;
             AtomicLong seed = this.seed;
             do {
             //(6)
             oldseed = seed.get();
             //(7)
             nextseed = (oldseed * multiplier + addend) & mask;
             //(8)
             } while (!seed.compareAndSet(oldseed, nextseed));
             //(9)
             return (int)(nextseed >>> (48 - bits));
             }
             代码（6）获取当前原子变量种子的值
             代码（7）根据当前种子值计算新的种子
             代码（8）使用CAS操作，使用新的种子去更新老的种子，多线程下可能多个线程都同时执行到了代码（6）那么可能多个线程都拿到的当前种子的值是同一个，然后执行步骤（7）计算的新种子也都是一样的，但是步骤（8）的CAS操作会保证只有一个线程可以更新老的种子为新的，失败的线程会通过循环从新获取更新后的种子作为当前种子去计算老的种子，可见这里解决了上面提到的问题，也就保证了随机数的随机性。
             代码（9）则使用固定算法根据新的种子计算随机数。
             总结下：每个Random实例里面有一个原子性的种子变量用来记录当前的种子的值，
             当要生成新的随机数时候要根据当前种子计算新的种子并更新回原子变量。多线程下使用单个Random实例生成随机数时候，
             多个线程同时计算随机数计算新的种子时候多个线程会竞争同一个原子变量的更新操作，由于原子变量的更新是CAS操作，
             同时只有一个线程会成功，所以会造成大量线程进行自旋重试，
             这是会降低并发性能的，所以ThreadLocalRandom应运而生。
             */
            counter = new AtomicInteger(ThreadLocalRandom.current().nextInt());
            //putIfAbsent   如果传入key对应的value已经存在，就返回存在的value，
            // 不进行替换。如果不存在，就添加key和value，返回null
            AtomicInteger currentCounter = topicCounterMap.putIfAbsent(topic, counter);
            if (currentCounter != null) {
                counter = currentCounter;
            }
        }
        return counter.getAndIncrement();
    }

    public void close() {}

}
