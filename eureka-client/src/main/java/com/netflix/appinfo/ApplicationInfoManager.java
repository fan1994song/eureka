/*
 * Copyright 2012 Netflix, Inc.
 *f
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.appinfo;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.netflix.appinfo.InstanceInfo.InstanceStatus;
import com.netflix.appinfo.providers.EurekaConfigBasedInstanceInfoProvider;
import com.netflix.discovery.StatusChangeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The class that initializes information required for registration with
 * <tt>Eureka Server</tt> and to be discovered by other components.
 *
 * <p>
 * The information required for registration is provided by the user by passing
 * the configuration defined by the contract in {@link EurekaInstanceConfig}
 * }.AWS clients can either use or extend {@link CloudInstanceConfig
 * }. Other non-AWS clients can use or extend either
 * {@link MyDataCenterInstanceConfig} or very basic
 * {@link AbstractInstanceConfig}.
 * </p>
 *
 *
 * @author Karthik Ranganathan, Greg Kim
 *
 */
@Singleton
public class ApplicationInfoManager {
    private static final Logger logger = LoggerFactory.getLogger(ApplicationInfoManager.class);

    private static final InstanceStatusMapper NO_OP_MAPPER = new InstanceStatusMapper() {
        @Override
        public InstanceStatus map(InstanceStatus prev) {
            return prev;
        }
    };

    private static ApplicationInfoManager instance = new ApplicationInfoManager(null, null, null);

    protected final Map<String, StatusChangeListener> listeners;
    private final InstanceStatusMapper instanceStatusMapper;

    private InstanceInfo instanceInfo;
    private EurekaInstanceConfig config;

    public static class OptionalArgs {
        private InstanceStatusMapper instanceStatusMapper;

        @com.google.inject.Inject(optional = true)
        public void setInstanceStatusMapper(InstanceStatusMapper instanceStatusMapper) {
            this.instanceStatusMapper = instanceStatusMapper;
        }

        InstanceStatusMapper getInstanceStatusMapper() {
            return instanceStatusMapper == null ? NO_OP_MAPPER : instanceStatusMapper;
        }
    }

    /**
     * public for DI use. This class should be in singleton scope so do not create explicitly.
     * Either use DI or create this explicitly using one of the other public constructors.
     */
    @Inject
    public ApplicationInfoManager(EurekaInstanceConfig config, InstanceInfo instanceInfo, OptionalArgs optionalArgs) {
        this.config = config;
        this.instanceInfo = instanceInfo;
        this.listeners = new ConcurrentHashMap<String, StatusChangeListener>();
        if (optionalArgs != null) {
            this.instanceStatusMapper = optionalArgs.getInstanceStatusMapper();
        } else {
            this.instanceStatusMapper = NO_OP_MAPPER;
        }

        // Hack to allow for getInstance() to use the DI'd ApplicationInfoManager
        instance = this;
    }

    public ApplicationInfoManager(EurekaInstanceConfig config, /* nullable */ OptionalArgs optionalArgs) {
        this(config, new EurekaConfigBasedInstanceInfoProvider(config).get(), optionalArgs);
    }

    public ApplicationInfoManager(EurekaInstanceConfig config, InstanceInfo instanceInfo) {
        this(config, instanceInfo, null);
    }

    /**
     * @deprecated 2016-09-19 prefer {@link #ApplicationInfoManager(EurekaInstanceConfig, com.netflix.appinfo.ApplicationInfoManager.OptionalArgs)}
     */
    @Deprecated
    public ApplicationInfoManager(EurekaInstanceConfig config) {
        this(config, (OptionalArgs) null);
    }

    /**
     * @deprecated please use DI instead
     */
    @Deprecated
    public static ApplicationInfoManager getInstance() {
        return instance;
    }

    public void initComponent(EurekaInstanceConfig config) {
        try {
            this.config = config;
            this.instanceInfo = new EurekaConfigBasedInstanceInfoProvider(config).get();
        } catch (Throwable e) {
            throw new RuntimeException("Failed to initialize ApplicationInfoManager", e);
        }
    }

    /**
     * Gets the information about this instance that is registered with eureka.
     *
     * @return information about this instance that is registered with eureka.
     */
    public InstanceInfo getInfo() {
        return instanceInfo;
    }

    public EurekaInstanceConfig getEurekaInstanceConfig() {
        return config;
    }

    /**
     * Register user-specific instance meta data. Application can send any other
     * additional meta data that need to be accessed for other reasons.The data
     * will be periodically sent to the eureka server.
     *
     * Please Note that metadata added via this method is not guaranteed to be submitted
     * to the eureka servers upon initial registration, and may be submitted as an update
     * at a subsequent time. If you want guaranteed metadata for initial registration,
     * please use the mechanism described in {@link EurekaInstanceConfig#getMetadataMap()}
     *
     * @param appMetadata application specific meta data.
     */
    public void registerAppMetadata(Map<String, String> appMetadata) {
        instanceInfo.registerRuntimeMetadata(appMetadata);
    }

    /**
     * Set the status of this instance. Application can use this to indicate
     * whether it is ready to receive traffic. Setting the status here also notifies all registered listeners
     * of a status change event.
     * 当实例状态变更时,通过观察者模式，以事件监听的形式去通知，监听者完成状态变更的业务操作
     * 在此处，会通过InstanceInfoReplicator发起实例状态变更的HTTP请求，完成对eureka Server的通知操作
     * @param status Status of the instance
     */
    public synchronized void setInstanceStatus(InstanceStatus status) {
        InstanceStatus next = instanceStatusMapper.map(status);
        if (next == null) {
            return;
        }

        InstanceStatus prev = instanceInfo.setStatus(next);
        if (prev != null) {
            for (StatusChangeListener listener : listeners.values()) {
                try {
                    listener.notify(new StatusChangeEvent(prev, next));
                } catch (Exception e) {
                    logger.warn("failed to notify listener: {}", listener.getId(), e);
                }
            }
        }
    }

    public void registerStatusChangeListener(StatusChangeListener listener) {
        listeners.put(listener.getId(), listener);
    }

    public void unregisterStatusChangeListener(String listenerId) {
        listeners.remove(listenerId);
    }

    /**
     * Refetches the hostname to check if it has changed. If it has, the entire
     * <code>DataCenterInfo</code> is refetched and passed on to the eureka
     * server on next heartbeat.
     * 重新获取主机名以检查它是否已更改。 如果有，整个DataCenterInfo被重新获取并在下一次心跳时传递给 eureka 服务器
     * see {@link InstanceInfo#getHostName()} for explanation on why the hostname is used as the default address
     */
    public void refreshDataCenterInfoIfRequired() {
        String existingAddress = instanceInfo.getHostName();

        String existingSpotInstanceAction = null;
        if (instanceInfo.getDataCenterInfo() instanceof AmazonInfo) {
            existingSpotInstanceAction = ((AmazonInfo) instanceInfo.getDataCenterInfo()).get(AmazonInfo.MetaDataKey.spotInstanceAction);
        }

        // 读取实例最新配置
        String newAddress;
        if (config instanceof RefreshableInstanceConfig) {
            // Refresh data center info, and return up to date address
            // 刷新数据中心信息，并返回最新地址
            newAddress = ((RefreshableInstanceConfig) config).resolveDefaultAddress(true);
        } else {
            /**
             * 一般情况下，我们使用的是非 RefreshableInstanceConfig 实现的配置类( 一般是 MyDataCenterInstanceConfig )，
             * 因为 AbstractInstanceConfig.hostInfo 是静态属性，即使本机修改了 IP 等信息，Eureka-Client 进程也不会感知到
             */
            newAddress = config.getHostName(true);
        }
        String newIp = config.getIpAddress();

        // 若新地址不等于已存在的地址，更新实例的hostname、ip、数据中心
        if (newAddress != null && !newAddress.equals(existingAddress)) {
            logger.warn("The address changed from : {} => {}", existingAddress, newAddress);
            updateInstanceInfo(newAddress, newIp);
        }

        if (config.getDataCenterInfo() instanceof AmazonInfo) {
            String newSpotInstanceAction = ((AmazonInfo) config.getDataCenterInfo()).get(AmazonInfo.MetaDataKey.spotInstanceAction);
            if (newSpotInstanceAction != null && !newSpotInstanceAction.equals(existingSpotInstanceAction)) {
                logger.info(String.format("The spot instance termination action changed from: %s => %s",
                        existingSpotInstanceAction,
                        newSpotInstanceAction));
                updateInstanceInfo(null , null );
            }
        }        
    }

    private void updateInstanceInfo(String newAddress, String newIp) {
        // :( in the legacy code here the builder is acting as a mutator.
        // This is hard to fix as this same instanceInfo instance is referenced elsewhere.
        // We will most likely re-write the client at sometime so not fixing for now.
        InstanceInfo.Builder builder = new InstanceInfo.Builder(instanceInfo);
        if (newAddress != null) {
            builder.setHostName(newAddress);
        }
        if (newIp != null) {
            builder.setIPAddr(newIp);
        }
        builder.setDataCenterInfo(config.getDataCenterInfo());
        instanceInfo.setIsDirty();
    }

    public void refreshLeaseInfoIfRequired() {
        LeaseInfo leaseInfo = instanceInfo.getLeaseInfo();
        if (leaseInfo == null) {
            return;
        }
        // 移除实例时间间隔（90秒）、发送心跳频率（30秒），若不一样，刷新变更当前实例的续租实例属性
        int currentLeaseDuration = config.getLeaseExpirationDurationInSeconds();
        int currentLeaseRenewal = config.getLeaseRenewalIntervalInSeconds();
        if (leaseInfo.getDurationInSecs() != currentLeaseDuration || leaseInfo.getRenewalIntervalInSecs() != currentLeaseRenewal) {
            LeaseInfo newLeaseInfo = LeaseInfo.Builder.newBuilder()
                    .setRenewalIntervalInSecs(currentLeaseRenewal)
                    .setDurationInSecs(currentLeaseDuration)
                    .build();
            instanceInfo.setLeaseInfo(newLeaseInfo);
            instanceInfo.setIsDirty();
        }
    }

    public static interface StatusChangeListener {
        String getId();

        void notify(StatusChangeEvent statusChangeEvent);
    }

    public static interface InstanceStatusMapper {

        /**
         * given a starting {@link com.netflix.appinfo.InstanceInfo.InstanceStatus}, apply a mapping to return
         * the follow up status, if applicable.
         *
         * @return the mapped instance status, or null if the mapping is not applicable.
         */
        InstanceStatus map(InstanceStatus prev);
    }

}
