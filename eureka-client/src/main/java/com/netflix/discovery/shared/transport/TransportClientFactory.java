package com.netflix.discovery.shared.transport;

import com.netflix.discovery.shared.resolver.EurekaEndpoint;

/**
 * A low level client factory interface. Not advised to be used by top level consumers.
 *
 * @author David Liu
 */
public interface TransportClientFactory {

    /**
     * 创建 EurekaHttpClient
     * @param serviceUrl
     * @return
     */
    EurekaHttpClient newClient(EurekaEndpoint serviceUrl);

    /**
     * 关闭工厂
     */
    void shutdown();

}
