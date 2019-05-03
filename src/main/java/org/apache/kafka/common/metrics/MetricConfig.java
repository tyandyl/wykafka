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
package org.apache.kafka.common.metrics;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *说起kafka的metrics,很多人应该是即陌生又熟悉,
 熟悉是因为阅读源码的过程中,不可避免地会看到metrics.add()的代码.而陌生是因为metrics仅仅只是辅助功能,并不是kafka主要逻辑的一部分,
 并不会引起读者太多的关注.
 同时网上关于metrics这一块的分析也较少,这篇文章就带着大家一探metrics的究竟.
 在这里首先说明一个容易产生误解的地方,不少文章说kafka使用yammers框架来实现性能监控.这么说其实没有问题,
 因为kafka确实通过yammers向外暴露了接口,可以通过jmx或者grahite来监视各个性能参数.但是kafka内的性能监控比如producer,consumer的配额限制,
 并不是通过yammer实现的.而是通过自己的一套metrics框架来实现的.
 事实上,kafka有两个metrics包,在看源码的时候很容易混淆
 package kafka.metrics 以及package org.apache.kafka.common.metrics
 可以看到这两个包的包名都是metrics,但是他们负责的任务并不相同,而且两个包中的类并没有任何的互相引用关系.可以看作是两个完全独立的包.
 kafka.mtrics这个包,主要调用yammer的Api,并进行封装,提供给client监测kafka的各个性能参数.
 而commons.metrics这个包是我这篇文章主要要介绍的,这个包并不是面向client提供服务的,
 他是为了给kafka中的其他组件,比如replicaManager,PartitionManager,QuatoManager提供调用,让这些Manager了解kafka现在的运行状况,
 以便作出相应决策的.
 首先metrics第一次被初始化,在kafkaServer的startup()方法中
 metrics = new Metrics(metricConfig, reporters, kafkaMetricsTime, true)
 quotaManagers = QuotaFactory.instantiate(config, metrics, time)
 初始化了一个Metrics,并将这个实例传到quotaManagers的构造函数中,这里简单介绍一下quotaManagers.
 这是kafka中用来限制kafka,producer的传输速度的,比如在config文件下设置producer不能以超过5MB/S的速度传输数据,
 那么这个限制就是通过quotaManager来实现的.
查看：Metrics 类
 */
public class MetricConfig {

    private Quota quota;
    private int samples;//通过metrics.num.samples设置度量个数，默认值2，
    private long eventWindow;
    private long timeWindowMs;//通过metrics.sample.window.ms设置度量信息有效时间，默认30000毫秒
    private Map<String, String> tags;
    private Sensor.RecordingLevel recordingLevel;

    public MetricConfig() {
        super();
        this.quota = null;
        this.samples = 2;
        this.eventWindow = Long.MAX_VALUE;
        this.timeWindowMs = TimeUnit.MILLISECONDS.convert(30, TimeUnit.SECONDS);
        this.tags = new LinkedHashMap<>();
        this.recordingLevel = Sensor.RecordingLevel.INFO;
    }

    public Quota quota() {
        return this.quota;
    }

    public MetricConfig quota(Quota quota) {
        this.quota = quota;
        return this;
    }

    public long eventWindow() {
        return eventWindow;
    }

    public MetricConfig eventWindow(long window) {
        this.eventWindow = window;
        return this;
    }

    public long timeWindowMs() {
        return timeWindowMs;
    }

    public MetricConfig timeWindow(long window, TimeUnit unit) {
        this.timeWindowMs = TimeUnit.MILLISECONDS.convert(window, unit);
        return this;
    }

    public Map<String, String> tags() {
        return this.tags;
    }

    public MetricConfig tags(Map<String, String> tags) {
        this.tags = tags;
        return this;
    }

    public int samples() {
        return this.samples;
    }

    public MetricConfig samples(int samples) {
        if (samples < 1)
            throw new IllegalArgumentException("The number of samples must be at least 1.");
        this.samples = samples;
        return this;
    }

    public Sensor.RecordingLevel recordLevel() {
        return this.recordingLevel;
    }

    public MetricConfig recordLevel(Sensor.RecordingLevel recordingLevel) {
        this.recordingLevel = recordingLevel;
        return this;
    }


}
