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

import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

// NOTE:MQ容错策略
// Producer使用该类的selectOneMessageQueue选择一个MessageQueue发送消息
public class MQFaultStrategy {
    private final static InternalLogger log = ClientLogger.getLog();

    // 使用LatencyFaultToleranceImpl做为延迟容错的实现
    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();

    // 是否开启延迟容错策略，默认不开启
    // 开启后，使用latencyFaultTolerance管理broker的延迟和可用状态，否则轮询TopicPublishInfo
    private boolean sendLatencyFaultEnable = false;

    // 开启sendLatencyFaultEnable后适用
    // 定义两个数组，第一个数组是最大延迟，第二个数组是对应的不可用时间
    // 当某个broker返回的响应时间比较慢，比如1200ms，则会设置该broker不可用时间，对应的是60000L，即60秒
    // 延迟在550ms以下都是正常，不需要设置不可用时间；后续都会设置（包括550ms）
    private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};
    private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};

    public long[] getNotAvailableDuration() {
        return notAvailableDuration;
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.notAvailableDuration = notAvailableDuration;
    }

    public long[] getLatencyMax() {
        return latencyMax;
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.latencyMax = latencyMax;
    }

    public boolean isSendLatencyFaultEnable() {
        return sendLatencyFaultEnable;
    }

    // 设置sendLatencyFaultEnable开关
    // producer的同名方法会调用该方法，如需打开，在创建producer时设置
    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.sendLatencyFaultEnable = sendLatencyFaultEnable;
    }

    /**
     * 选择一个MessageQueue.
     *
     * @param tpInfo topic生产者的路由信息，包含messageQueue和broker信息
     * @param lastBrokerName 上次使用的broker. 不开启延迟容错时，会尽量选择不同的broker，
     * @return
     */
    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        if (this.sendLatencyFaultEnable) { // 开启了延迟容错
            try {
                // index++
                // 这个index在选择过程中加了几次，可能也是没办法吧 😓
                int index = tpInfo.getSendWhichQueue().getAndIncrement();
                // 根据列表长度循环，从index开始，获取tpInfo的MessageQueue
                // 使用latencyFaultTolerance判断MessageQueue是否可用
                // 可用的话直接返回,不可用下一条
                // 相比latencyFaultTolerance.pickOneAtLeast()，此处没有判断队列的writeQueueNums，因为组装生产者路由信息时，writeQueueNums已经大于0
                for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {
                    int pos = Math.abs(index++) % tpInfo.getMessageQueueList().size();
                    if (pos < 0)
                        pos = 0;
                    MessageQueue mq = tpInfo.getMessageQueueList().get(pos);
                    if (latencyFaultTolerance.isAvailable(mq.getBrokerName()))
                        return mq;
                }

                // 遍历完了，没有可用，就是说所有broker都有延迟
                // 通过延迟容错策略，凑合选一个吧
                // 从变量命名上可看出，这个不是延迟最好的broker，如果都是用最好的，就都发到一个里去了，容易撑爆
                // 事实上，latencyFaultTolerance里根据延迟排序，在前一半（即较好的一半）中使用index轮询
                final String notBestBroker = latencyFaultTolerance.pickOneAtLeast();

                // 选择出来后，判断该broker的writeQueueNums，如果大于0则可用，从broker中选择一个队列；否则循环结束后，使用tpInfo.selectOneMessageQueue()选择
                int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker);
                if (writeQueueNums > 0) {
                    final MessageQueue mq = tpInfo.selectOneMessageQueue();
                    if (notBestBroker != null) {
                        mq.setBrokerName(notBestBroker);
                        // 这是getAndIncrease的第二次，如果一直走这个逻辑，只能选2、4、6、8之类的了....
                        mq.setQueueId(tpInfo.getSendWhichQueue().getAndIncrement() % writeQueueNums);
                    }
                    return mq;
                } else {
                    // 这个broker不可些，从latencyFaultTolerance中删除之
                    // 这里没有循环，不可用就放弃了，走tpInfo.selectOneMessageQueue()的逻辑
                    latencyFaultTolerance.remove(notBestBroker);
                }
            } catch (Exception e) {
                log.error("Error occurred when selecting message queue", e);
            }
            // 上述的方式没选出来，用tpInfo.selectOneMessageQueue()认命的选一个吧
            return tpInfo.selectOneMessageQueue();
        }

        // 没开始延迟容错功能时，直接使用tpInfo的选择，并尽量避开lastBrokerName
        return tpInfo.selectOneMessageQueue(lastBrokerName);
    }

    // 更新broker的延迟
    // isolation=true时，计算不可用时间时不使用currentLatency，而直接使用常量30000，这会隔离该broker10分钟
    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        if (this.sendLatencyFaultEnable) {
            long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);
            // 更新延迟
            // FIXME: 这个更新延迟使用了currentLatency而不是duration，感觉不是很恰当
            // FaultItem排序时currentLatency优先于startTime(即system time + duration)，故isolation=true但currentLatency较小的broker会排在前面
            // 感觉isolation=true的broker应该放后面
            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
        }
    }

    private long computeNotAvailableDuration(final long currentLatency) {
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            if (currentLatency >= latencyMax[i])
                return this.notAvailableDuration[i];
        }

        return 0;
    }
}
