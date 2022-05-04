/*
 * Copyright 2015 Netflix, Inc.
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

package com.netflix.eureka.util.batcher;

import com.netflix.eureka.util.batcher.TaskProcessor.ProcessingResult;

/**
 * {@link TrafficShaper} provides admission control policy prior to dispatching tasks to workers.
 * It reacts to events coming via reprocess requests (transient failures, congestion), and delays the processing
 * depending on this feedback.
 *
 * @author Tomasz Bak
 */
class TrafficShaper {

    /**
     * Upper bound on delay provided by configuration.
     * 最大延迟30秒
     */
    private static final long MAX_DELAY = 30 * 1000;

    private final long congestionRetryDelayMs;
    private final long networkFailureRetryMs;

    /**
     * 最后请求限流时间戳：毫秒
     */
    private volatile long lastCongestionError;
    /**
     * 最后网络失败时间戳：毫秒
     */
    private volatile long lastNetworkFailure;

    /**
     * 创建网络通信器时，最大延迟不能超过30秒
     *
     * @param congestionRetryDelayMs
     * @param networkFailureRetryMs
     */
    TrafficShaper(long congestionRetryDelayMs, long networkFailureRetryMs) {
        this.congestionRetryDelayMs = Math.min(MAX_DELAY, congestionRetryDelayMs);
        this.networkFailureRetryMs = Math.min(MAX_DELAY, networkFailureRetryMs);
    }

    void registerFailure(ProcessingResult processingResult) {
        if (processingResult == ProcessingResult.Congestion) {
            lastCongestionError = System.currentTimeMillis();
        } else if (processingResult == ProcessingResult.TransientError) {
            lastNetworkFailure = System.currentTimeMillis();
        }
    }

    /**
     * 计算提交延迟：毫秒
     *
     * @return
     */
    long transmissionDelay() {
        /**
         * 无延迟
         */
        if (lastCongestionError == -1 && lastNetworkFailure == -1) {
            return 0;
        }

        long now = System.currentTimeMillis();
        // 计算最后请求限流带来的延迟
        if (lastCongestionError != -1) {
            long congestionDelay = now - lastCongestionError;
            if (congestionDelay >= 0 && congestionDelay < congestionRetryDelayMs) {
                // 计算任务执行所需延迟时间
                return congestionRetryDelayMs - congestionDelay;
            }
            // 下次重置
            lastCongestionError = -1;
        }

        //计算最后网络失败带来的延迟
        if (lastNetworkFailure != -1) {
            long failureDelay = now - lastNetworkFailure;
            if (failureDelay >= 0 && failureDelay < networkFailureRetryMs) {
                // 计算任务执行所需延迟时间
                return networkFailureRetryMs - failureDelay;
            }
            // 下次重置
            lastNetworkFailure = -1;
        }
        return 0;
    }
}
