/*
 * Copyright 2014 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.discovery.util;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Rate limiter implementation is based on token bucket algorithm. There are two parameters:
 * <ul>
 * <li>
 *     burst size - maximum number of requests allowed into the system as a burst
 * </li>
 * <li>
 *     average rate - expected number of requests per second (RateLimiters using MINUTES is also supported)
 * </li>
 * </ul>
 *
 * @author Tomasz Bak
 */
public class RateLimiter {

    /**
     * 速率单位状态为毫秒值
     */
    private final long rateToMsConversion;

    private final AtomicInteger consumedTokens = new AtomicInteger();
    private final AtomicLong lastRefillTime = new AtomicLong(0);

    @Deprecated
    public RateLimiter() {
        this(TimeUnit.SECONDS);
    }

    public RateLimiter(TimeUnit averageRateUnit) {
        switch (averageRateUnit) {
            case SECONDS:
                rateToMsConversion = 1000;
                break;
            case MINUTES:
                rateToMsConversion = 60 * 1000;
                break;
            default:
                throw new IllegalArgumentException("TimeUnit of " + averageRateUnit + " is not supported");
        }
    }

    /**
     * 获取令牌
     *
     * @param burstSize   令牌桶上限
     * @param averageRate 令牌装入平均速率
     * @return 获取结果
     * <p>
     * averageRateUnit = SECONDS
     * averageRate = 2000
     * burstSize = 10
     * 每秒可获取 2000 个令牌。例如，每秒允许请求 2000 次。
     * 每毫秒可填充 2000 / 1000 = 2 个消耗的令牌。
     * 每毫秒可获取 10 个令牌。例如，每毫秒允许请求上限为 10 次，并且请求消耗掉的令牌，需要逐步填充。这里要注意下，虽然每毫秒允许请求上限为 10 次，这是在没有任何令牌被消耗的情况下，实际每秒允许请求依然是 2000 次。
     * 这就是基于令牌桶算法的限流的特点：让流量平稳，而不是瞬间流量。1000 QPS 相对平均的分摊在这一秒内，而不是第 1 ms 999 请求，后面 999 ms 0 请求
     */
    public boolean acquire(int burstSize, long averageRate) {
        return acquire(burstSize, averageRate, System.currentTimeMillis());
    }

    /**
     * 获取令牌
     *
     * @param burstSize         令牌桶上限 10
     * @param averageRate       令牌装入平均速率 2000
     * @param currentTimeMillis 当前毫秒值
     * @return
     */
    public boolean acquire(int burstSize, long averageRate, long currentTimeMillis) {
        if (burstSize <= 0 || averageRate <= 0) { // Instead of throwing exception, we just let all the traffic go
            return true;
        }

        // 填充令牌
        refillToken(burstSize, averageRate, currentTimeMillis);
        // 消费令牌
        return consumeToken(burstSize);
    }

    /**
     * 减少消费的token数值
     * @param burstSize
     * @param averageRate
     * @param currentTimeMillis
     */
    private void refillToken(int burstSize, long averageRate, long currentTimeMillis) {
        /**
         * 计算填充令牌时间差值
         */
        long refillTime = lastRefillTime.get();
        long timeDelta = currentTimeMillis - refillTime;

        /**
         * 时间差内，产生的新的令牌数
         */
        long newTokens = timeDelta * averageRate / rateToMsConversion;
        if (newTokens > 0) {
            // 当前填充时间值
            long newRefillTime = refillTime == 0
                    ? currentTimeMillis
                    // 不直接加timeDelta是因为,产生newTokens所需要的时间值不一定完全等于timeDelta(有余数)
                    : refillTime + newTokens * rateToMsConversion / averageRate;
            if (lastRefillTime.compareAndSet(refillTime, newRefillTime)) {
                while (true) {
                    // 当前消费令牌
                    int currentLevel = consumedTokens.get();
                    // burstSize 可能调小，例如，系统接入分布式配置中心，可以远程调整该数值
                    // 消费令牌数和桶大小取小的那个，例:(Math.min(1000,10)),以单位时间内产生的令牌数值为准
                    int adjustedLevel = Math.min(currentLevel, burstSize); // In case burstSize decreased
                    // 减去产生的token，就是当前已经消费的令牌
                    int newLevel = (int) Math.max(0, adjustedLevel - newTokens);
                    // 设置当前已消费的令牌数
                    if (consumedTokens.compareAndSet(currentLevel, newLevel)) {
                        return;
                    }
                }
            }
        }
    }

    private boolean consumeToken(int burstSize) {
        while (true) {
            int currentLevel = consumedTokens.get();
            if (currentLevel >= burstSize) {
                return false;
            }
            if (consumedTokens.compareAndSet(currentLevel, currentLevel + 1)) {
                return true;
            }
        }
    }

    public void reset() {
        consumedTokens.set(0);
        lastRefillTime.set(0);
    }
}
