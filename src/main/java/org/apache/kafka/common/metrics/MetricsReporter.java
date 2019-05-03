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

import java.util.List;

import org.apache.kafka.common.Configurable;

/**
 * JMX(Java Management Extensions)是一个为应用程序植入管理功能的框架。
 * JMX是一套标准的代理和服务，实际上，用户可以在任何Java应用程序中使用这些代理和服务实现管理。
 * 这是官方文档上的定义，我看过很多次也无法很好的理解。我个人的理解是JMX让程序有被管理的功能，
 * 例如你开发一个WEB网站，它是在24小时不间断运行，那么你肯定会对网站进行监控，如每天的UV、PV是多少；
 * 又或者在业务高峰的期间，你想对接口进行限流，就必须去修改接口并发的配置值。
 *
 * 应用场景：中间件软件WebLogic的管理页面就是基于JMX开发的，而JBoss则整个系统都基于JMX构架。

 对于一些参数的修改，网上有一段描述还是比较形象的：

 1、程序初哥一般是写死在程序中，到要改变的时候就去修改代码，然后重新编译发布。

 2、程序熟手则配置在文件中（JAVA一般都是properties文件），到要改变的时候只要修改配置文件，但还是必须重启系统，以便读取配置文件里最新的值。

 3、程序好手则会写一段代码，把配置值缓存起来，系统在获取的时候，先看看配置文件有没有改动，如有改动则重新从配置里读取，否则从缓存里读取。

 4、程序高手则懂得物为我所用，用JMX把需要配置的属性集中在一个类中，然后写一个MBean，再进行相关配置。另外JMX还提供了一个工具页，以方便我们对参数值进行修改。

 */

//实现步骤：第一步：首先定义一个接口
//Metrics 提供了 Report 接口，用于展示 metrics 获取到的统计数
public interface MetricsReporter extends Configurable, AutoCloseable {

    /**
     * This is called when the reporter is first registered to initially register all existing metrics
     * @param metrics All currently existing metrics
     */
    public void init(List<KafkaMetric> metrics);

    /**
     * This is called whenever a metric is updated or added
     * @param metric
     */
    public void metricChange(KafkaMetric metric);

    /**
     * This is called whenever a metric is removed
     * @param metric
     */
    public void metricRemoval(KafkaMetric metric);

    /**
     * Called when the metrics repository is closed.
     */
    public void close();

}
