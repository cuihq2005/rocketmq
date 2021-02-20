/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.client.latency;

// NOTE: 延迟容错
public interface LatencyFaultTolerance<T> {
    /**
     * 更新broker的延迟及非可用时间.
     *
     * @param name broker的名称
     * @param currentLatency 本次调用的延迟
     * @param notAvailableDuration 非可用时间
     */
    void updateFaultItem(final T name, final long currentLatency, final long notAvailableDuration);

    /**
     * 判断broker是否可用.
     * producer选择MessageQueue后，会调用该方法判断broker是否可用，不可用继续选择
     * @param name broker名称
     * @return 延迟容错服务持有该broker，且当前时间大于不可用时间（updateFaultItem时的system time + notAvailableDuration）
     */
    boolean isAvailable(final T name);

    /**
     * 从LatencyFaultTolerance中删除一个broker.
     *
     * @param name
     */
    void remove(final T name);

    /**
     * 在所有的broker都不available时，瘸子里挑将军，选择一个broker.
     * @return
     */
    T pickOneAtLeast();
}
