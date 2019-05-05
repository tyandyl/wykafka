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
package org.apache.kafka.common.security;

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.network.ListenerName;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * 实现一个完整的用户安全管理框架，可以有两种实现途径。在J2EE出现以前，大部分是由应用系统本身实现。
 * 因此，很多有经验的软件商都拥有自己成熟的用户安全管理系统，但是缺点也是比较明显，自己设计的用户安全管理系统可重用性低，
 * 有的和具体应用程序过分紧密地绑定在一起，无法移植到其他系统上。
 * 但是，随着业务量上升，应用系统的不断增加，会出现不同应用系统拥有不同的用户登录验证体系，很显然，这给用户访问带来了不方便，
 * 用户不可能为每个系统注册一套用户和密码，定制一套用户角色。因此，整个服务器群中需要统一的用户权限验证体系。
 * 而J2EE容器的用户安全管理框架再辅助以LDAP或数据库系统，可以很方便地达到这个目标。
 *
 * J2EE容器的用户安全框架是基于RBAC（Roled-Based Access Control，相关网址：http://csrc.nist.gov/rbac/）设计模型建立的，这是一个基于角色的访问权限控制模型。
 *
 * 首先必须了解角色的含义，在RBAC中角色Role的定义是：Role是明确表达访问控制（Aceess Control）策略的一种语义构建词。
 *
 * 角色可以是指做某些事情的资格，比如医生或物理学家；也可以包含权力和责任的意思，如部门经理或局长等。角色和组（groups）是有区别的。组就是纯粹代表一群用户；角色一方面代表一系列用户，另外一方面可以代表一系列权限，因此可以说是用户和权限的结合体。
 *
 * 引入角色的概念主要是为了分离用户和访问权限的直接联系。用户与访问权限的直接组合可能是短暂的，而角色则可以相对稳定，因为一个系统中和角色相关的权限变化是有限的。
 *
 * 在RBAC理论出现之前，很多人都是把用户和权限混淆在一起，这样当用户或权限发生变化时，都会涉及到对方，很显然这在实际实现中将是非常复杂的。所以诞生RBAC，创造了一个“角色”的名词，注意这是人为创造的语义词。角色就是用户和权限之间的第3者，通过引入角色概念，将用户和权限的关系解耦。这样用户的变化只要涉及到角色就可以，无需考虑权限。而权限的变化只涉及到角色，无需考虑用户或用户组。
 *
 * 因此，基于角色的访问控制系统可以分为两个部分：与角色相关的访问权限系统以及与角色相关的用户管理系统。这样，通过角色这个中间者，将用户和权限联系在一起。其实这也非常符合日常生活的逻辑，例如王三来到某公司做业务员，公司章程规定了业务员一定的权限和职责，王三进入了业务员的角色，王三也就有了这些权限和职责，但这些权限职责不是和王三本人有直接联系的，而是通过王三的角色才会发生在王三身上；如果王三升迁做经理，表示其进入经理这样的角色，由此经理角色拥有的权限和职责王三又会拥有。
 *
 * 由于有了这样两个分离的系统，因此在具体应用上可以分别实现，在J2EE中，与角色相关的访问权限是通过配置文件（Web.xml和ejb-jar.xml）由容器自动实现的，而且这种访问权限的配置也是非常方便灵活的。
 *
 * 而与角色相关的用户系统则由具体应用系统的开发者来实现，可以采取基于数据库或LDAP等技术的数据系统来实现，例如用户注册资料的新增和修改等。
 *
 * 本项目的设计思路就是完全按照这两种分离的思路实现的，将与角色相关的访问权限系统交由J2EE容器实现。因此，如何配置J2EE将是本项目实现中的一个主要部分；代码设计编程则主要集中在基于数据库的用户管理系统上。
 *
 */
public class JaasContext {


    private static final String GLOBAL_CONTEXT_NAME_SERVER = "KafkaServer";
    private static final String GLOBAL_CONTEXT_NAME_CLIENT = "KafkaClient";

    /**
     * Returns an instance of this class.
     *
     * The context will contain the configuration specified by the JAAS configuration property
     * {@link SaslConfigs#SASL_JAAS_CONFIG} with prefix `listener.name.{listenerName}.{mechanism}.`
     * with listenerName and mechanism in lower case. The context `KafkaServer` will be returned
     * with a single login context entry loaded from the property.
     * <p>
     * If the property is not defined, the context will contain the default Configuration and
     * the context name will be one of:
     * <ol>
     *   <li>Lowercased listener name followed by a period and the string `KafkaServer`</li>
     *   <li>The string `KafkaServer`</li>
     *  </ol>
     * If both are valid entries in the default JAAS configuration, the first option is chosen.
     * </p>
     *
     * @throws IllegalArgumentException if listenerName or mechanism is not defined.
     */
    public static JaasContext loadServerContext(ListenerName listenerName, String mechanism, Map<String, ?> configs) {
        if (listenerName == null)
            throw new IllegalArgumentException("listenerName should not be null for SERVER");
        if (mechanism == null)
            throw new IllegalArgumentException("mechanism should not be null for SERVER");
        String globalContextName = GLOBAL_CONTEXT_NAME_SERVER;
        String listenerContextName = listenerName.value().toLowerCase(Locale.ROOT) + "." + GLOBAL_CONTEXT_NAME_SERVER;
        Password dynamicJaasConfig = (Password) configs.get(mechanism.toLowerCase(Locale.ROOT) + "." + SaslConfigs.SASL_JAAS_CONFIG);
        if (dynamicJaasConfig == null && configs.get(SaslConfigs.SASL_JAAS_CONFIG) != null)
            LOG.warn("Server config {} should be prefixed with SASL mechanism name, ignoring config", SaslConfigs.SASL_JAAS_CONFIG);
        return load(Type.SERVER, listenerContextName, globalContextName, dynamicJaasConfig);
    }

    /**
     * Returns an instance of this class.
     *
     * If JAAS configuration property @link SaslConfigs#SASL_JAAS_CONFIG} is specified,
     * the configuration object is created by parsing the property value. Otherwise, the default Configuration
     * is returned. The context name is always `KafkaClient`.
     *
     */
    public static JaasContext loadClientContext(Map<String, ?> configs) {
        String globalContextName = GLOBAL_CONTEXT_NAME_CLIENT;
        Password dynamicJaasConfig = (Password) configs.get(SaslConfigs.SASL_JAAS_CONFIG);
        return load(JaasContext.Type.CLIENT, null, globalContextName, dynamicJaasConfig);
    }

    static JaasContext load(JaasContext.Type contextType, String listenerContextName,
                            String globalContextName, Password dynamicJaasConfig) {
        if (dynamicJaasConfig != null) {
            JaasConfig jaasConfig = new JaasConfig(globalContextName, dynamicJaasConfig.value());
            AppConfigurationEntry[] contextModules = jaasConfig.getAppConfigurationEntry(globalContextName);
            if (contextModules == null || contextModules.length == 0)
                throw new IllegalArgumentException("JAAS config property does not contain any login modules");
            else if (contextModules.length != 1)
                throw new IllegalArgumentException("JAAS config property contains " + contextModules.length + " login modules, should be 1 module");
            return new JaasContext(globalContextName, contextType, jaasConfig, dynamicJaasConfig);
        } else
            return defaultContext(contextType, listenerContextName, globalContextName);
    }

    private static JaasContext defaultContext(JaasContext.Type contextType, String listenerContextName,
                                              String globalContextName) {
        String jaasConfigFile = System.getProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM);
        if (jaasConfigFile == null) {
            if (contextType == Type.CLIENT) {
                LOG.debug("System property '" + JaasUtils.JAVA_LOGIN_CONFIG_PARAM + "' and Kafka SASL property '" +
                        SaslConfigs.SASL_JAAS_CONFIG + "' are not set, using default JAAS configuration.");
            } else {
                LOG.debug("System property '" + JaasUtils.JAVA_LOGIN_CONFIG_PARAM + "' is not set, using default JAAS " +
                        "configuration.");
            }
        }

        Configuration jaasConfig = Configuration.getConfiguration();

        AppConfigurationEntry[] configEntries = null;
        String contextName = globalContextName;

        if (listenerContextName != null) {
            configEntries = jaasConfig.getAppConfigurationEntry(listenerContextName);
            if (configEntries != null)
                contextName = listenerContextName;
        }

        if (configEntries == null)
            configEntries = jaasConfig.getAppConfigurationEntry(globalContextName);

        if (configEntries == null) {
            String listenerNameText = listenerContextName == null ? "" : " or '" + listenerContextName + "'";
            String errorMessage = "Could not find a '" + globalContextName + "'" + listenerNameText + " entry in the JAAS " +
                    "configuration. System property '" + JaasUtils.JAVA_LOGIN_CONFIG_PARAM + "' is " +
                    (jaasConfigFile == null ? "not set" : jaasConfigFile);
            throw new IllegalArgumentException(errorMessage);
        }

        return new JaasContext(contextName, contextType, jaasConfig, null);
    }

    /**
     * The type of the SASL login context, it should be SERVER for the broker and CLIENT for the clients (consumer, producer,
     * etc.). This is used to validate behaviour (e.g. some functionality is only available in the broker or clients).
     */
    public enum Type { CLIENT, SERVER }

    private final String name;
    private final Type type;
    private final Configuration configuration;
    private final List<AppConfigurationEntry> configurationEntries;
    private final Password dynamicJaasConfig;

    public JaasContext(String name, Type type, Configuration configuration, Password dynamicJaasConfig) {
        this.name = name;
        this.type = type;
        this.configuration = configuration;
        AppConfigurationEntry[] entries = configuration.getAppConfigurationEntry(name);
        if (entries == null)
            throw new IllegalArgumentException("Could not find a '" + name + "' entry in this JAAS configuration.");
        this.configurationEntries = Collections.unmodifiableList(new ArrayList<>(Arrays.asList(entries)));
        this.dynamicJaasConfig = dynamicJaasConfig;
    }

    public String name() {
        return name;
    }

    public Type type() {
        return type;
    }

    public Configuration configuration() {
        return configuration;
    }

    public List<AppConfigurationEntry> configurationEntries() {
        return configurationEntries;
    }

    public Password dynamicJaasConfig() {
        return dynamicJaasConfig;
    }

    /**
     * Returns the configuration option for <code>key</code> from this context.
     * If login module name is specified, return option value only from that module.
     */
    public static String configEntryOption(List<AppConfigurationEntry> configurationEntries, String key, String loginModuleName) {
        for (AppConfigurationEntry entry : configurationEntries) {
            if (loginModuleName != null && !loginModuleName.equals(entry.getLoginModuleName()))
                continue;
            Object val = entry.getOptions().get(key);
            if (val != null)
                return (String) val;
        }
        return null;
    }

}
