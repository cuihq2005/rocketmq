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
package org.apache.rocketmq.client.impl.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.common.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

/**
 * NOTE: RebalanceImpl
 *
 * 每个consumer拥有一个独立的rebalance实例
 *
 */
public abstract class RebalanceImpl {
    protected static final InternalLogger log = ClientLogger.getLog();
    // 保存每个MessageQueue的ProcessQueue
    protected final ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable = new ConcurrentHashMap<MessageQueue, ProcessQueue>(64);
    // 保存每个topic所有的MessageQueue
    // 该变量由MQClientInstance在updateTopicRouteInfoFromNameServer中调用consumer的updateTopicSubscribeInfo赋值
    protected final ConcurrentMap<String/* topic */, Set<MessageQueue>> topicSubscribeInfoTable =
        new ConcurrentHashMap<String, Set<MessageQueue>>();
    // 保存每个topic的订阅规则
    protected final ConcurrentMap<String /* topic */, SubscriptionData> subscriptionInner =
        new ConcurrentHashMap<String, SubscriptionData>();
    // 订阅组组名
    protected String consumerGroup;
    // 消息模式
    protected MessageModel messageModel;
    // 队列分配策略
    protected AllocateMessageQueueStrategy allocateMessageQueueStrategy;
    // MQClientInstance. consumer在start时获取MQClientInstance实例，set到rebalance实例中
    protected MQClientInstance mQClientFactory;

    public RebalanceImpl(String consumerGroup, MessageModel messageModel,
        AllocateMessageQueueStrategy allocateMessageQueueStrategy,
        MQClientInstance mQClientFactory) {
        this.consumerGroup = consumerGroup;
        this.messageModel = messageModel;
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
        this.mQClientFactory = mQClientFactory;
    }

    public void unlock(final MessageQueue mq, final boolean oneway) {
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);
        if (findBrokerResult != null) {
            UnlockBatchRequestBody requestBody = new UnlockBatchRequestBody();
            requestBody.setConsumerGroup(this.consumerGroup);
            requestBody.setClientId(this.mQClientFactory.getClientId());
            requestBody.getMqSet().add(mq);

            try {
                this.mQClientFactory.getMQClientAPIImpl().unlockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000, oneway);
                log.warn("unlock messageQueue. group:{}, clientId:{}, mq:{}",
                    this.consumerGroup,
                    this.mQClientFactory.getClientId(),
                    mq);
            } catch (Exception e) {
                log.error("unlockBatchMQ exception, " + mq, e);
            }
        }
    }

    // 这个方法貌似只有顺序消费才调用，暂时忽略
    public void unlockAll(final boolean oneway) {
        HashMap<String, Set<MessageQueue>> brokerMqs = this.buildProcessQueueTableByBrokerName();

        for (final Map.Entry<String, Set<MessageQueue>> entry : brokerMqs.entrySet()) {
            final String brokerName = entry.getKey();
            final Set<MessageQueue> mqs = entry.getValue();

            if (mqs.isEmpty())
                continue;

            FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, true);
            if (findBrokerResult != null) {
                UnlockBatchRequestBody requestBody = new UnlockBatchRequestBody();
                requestBody.setConsumerGroup(this.consumerGroup);
                requestBody.setClientId(this.mQClientFactory.getClientId());
                requestBody.setMqSet(mqs);

                try {
                    this.mQClientFactory.getMQClientAPIImpl().unlockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000, oneway);

                    for (MessageQueue mq : mqs) {
                        ProcessQueue processQueue = this.processQueueTable.get(mq);
                        if (processQueue != null) {
                            processQueue.setLocked(false);
                            log.info("the message queue unlock OK, Group: {} {}", this.consumerGroup, mq);
                        }
                    }
                } catch (Exception e) {
                    log.error("unlockBatchMQ exception, " + mqs, e);
                }
            }
        }
    }

    // 将MessageQueue按brokerName分组，同一个broker的MessageQueue在一个分组里
    // lock和unlock时，根据brokerName向broker发送MessageQueue的lock和unlock命令
    private HashMap<String/* brokerName */, Set<MessageQueue>> buildProcessQueueTableByBrokerName() {
        HashMap<String, Set<MessageQueue>> result = new HashMap<String, Set<MessageQueue>>();
        for (MessageQueue mq : this.processQueueTable.keySet()) {
            Set<MessageQueue> mqs = result.get(mq.getBrokerName());
            if (null == mqs) {
                mqs = new HashSet<MessageQueue>();
                result.put(mq.getBrokerName(), mqs);
            }

            mqs.add(mq);
        }

        return result;
    }

    // 这个方法貌似只有顺序消费才调用，暂时忽略
    public boolean lock(final MessageQueue mq) {
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);
        if (findBrokerResult != null) {
            LockBatchRequestBody requestBody = new LockBatchRequestBody();
            requestBody.setConsumerGroup(this.consumerGroup);
            requestBody.setClientId(this.mQClientFactory.getClientId());
            requestBody.getMqSet().add(mq);

            try {
                Set<MessageQueue> lockedMq =
                    this.mQClientFactory.getMQClientAPIImpl().lockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000);
                for (MessageQueue mmqq : lockedMq) {
                    ProcessQueue processQueue = this.processQueueTable.get(mmqq);
                    if (processQueue != null) {
                        processQueue.setLocked(true);
                        processQueue.setLastLockTimestamp(System.currentTimeMillis());
                    }
                }

                boolean lockOK = lockedMq.contains(mq);
                log.info("the message queue lock {}, {} {}",
                    lockOK ? "OK" : "Failed",
                    this.consumerGroup,
                    mq);
                return lockOK;
            } catch (Exception e) {
                log.error("lockBatchMQ exception, " + mq, e);
            }
        }

        return false;
    }

    // 这个方法貌似只有顺序消费才调用，暂时忽略
    public void lockAll() {
        HashMap<String, Set<MessageQueue>> brokerMqs = this.buildProcessQueueTableByBrokerName();

        Iterator<Entry<String, Set<MessageQueue>>> it = brokerMqs.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, Set<MessageQueue>> entry = it.next();
            final String brokerName = entry.getKey();
            final Set<MessageQueue> mqs = entry.getValue();

            if (mqs.isEmpty())
                continue;

            FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, true);
            if (findBrokerResult != null) {
                LockBatchRequestBody requestBody = new LockBatchRequestBody();
                requestBody.setConsumerGroup(this.consumerGroup);
                requestBody.setClientId(this.mQClientFactory.getClientId());
                requestBody.setMqSet(mqs);

                try {
                    Set<MessageQueue> lockOKMQSet =
                        this.mQClientFactory.getMQClientAPIImpl().lockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000);

                    for (MessageQueue mq : lockOKMQSet) {
                        ProcessQueue processQueue = this.processQueueTable.get(mq);
                        if (processQueue != null) {
                            if (!processQueue.isLocked()) {
                                log.info("the message queue locked OK, Group: {} {}", this.consumerGroup, mq);
                            }

                            processQueue.setLocked(true);
                            processQueue.setLastLockTimestamp(System.currentTimeMillis());
                        }
                    }
                    for (MessageQueue mq : mqs) {
                        if (!lockOKMQSet.contains(mq)) {
                            ProcessQueue processQueue = this.processQueueTable.get(mq);
                            if (processQueue != null) {
                                processQueue.setLocked(false);
                                log.warn("the message queue locked Failed, Group: {} {}", this.consumerGroup, mq);
                            }
                        }
                    }
                } catch (Exception e) {
                    log.error("lockBatchMQ exception, " + mqs, e);
                }
            }
        }
    }

    // doRebalance
    public void doRebalance(final boolean isOrder) {
        // 遍历订阅信息，对每个topic做rebalanceByTopic
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            for (final Map.Entry<String, SubscriptionData> entry : subTable.entrySet()) {
                final String topic = entry.getKey();
                try {
                    // 对每个topic做rebalance
                    this.rebalanceByTopic(topic, isOrder);
                } catch (Throwable e) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        log.warn("rebalanceByTopic Exception", e);
                    }
                }
            }
        }

        // rebalance后，subscriptionInner中包含了新的订阅信息
        // 此时调用该方法，比较subscriptionInner和processQueueTable，将processQueueTable中多余的ProcessQueue删除（topic层面）
        this.truncateMessageQueueNotMyTopic();
    }

    public ConcurrentMap<String, SubscriptionData> getSubscriptionInner() {
        return subscriptionInner;
    }

    // 对topic做rebalance
    private void rebalanceByTopic(final String topic, final boolean isOrder) {
        switch (messageModel) {
            case BROADCASTING: { // 忽略广播模式的
                Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
                if (mqSet != null) {
                    boolean changed = this.updateProcessQueueTableInRebalance(topic, mqSet, isOrder);
                    if (changed) {
                        this.messageQueueChanged(topic, mqSet, mqSet);
                        log.info("messageQueueChanged {} {} {} {}",
                            consumerGroup,
                            topic,
                            mqSet,
                            mqSet);
                    }
                } else {
                    log.warn("doRebalance, {}, but the topic[{}] not exist.", consumerGroup, topic);
                }
                break;
            }
            case CLUSTERING: { // 集群模式
                // 该topic所有队列列表
                Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
                // 使用consumerGroup查询该topic的consumerId列表
                List<String> cidAll = this.mQClientFactory.findConsumerIdList(topic, consumerGroup);
                if (null == mqSet) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        log.warn("doRebalance, {}, but the topic[{}] not exist.", consumerGroup, topic);
                    }
                }

                if (null == cidAll) {
                    log.warn("doRebalance, {} {}, get consumer id list failed", consumerGroup, topic);
                }

                if (mqSet != null && cidAll != null) {
                    List<MessageQueue> mqAll = new ArrayList<MessageQueue>();
                    mqAll.addAll(mqSet);

                    Collections.sort(mqAll); // 排序所有messageQueue
                    Collections.sort(cidAll); // 排序所有的consumerId

                    // 根据分配策略，分配本consumer应该订阅的MessageQueue
                    // 默认的分配策略是AllocateMessageQueueAveragely，即平均分配
                    // 分配策略如下：用队列总数除以消费者总数，求出每个消费者应该消费几个队列，比如12个队列，3个消费者，则将12个队列分3份，每份4个队列
                    // 查找自己在cidAll的第几个，比如第二个，则自己取第二份的4个队列消费
                    // 由于mqAll和cidAll都是按同一规则排序，故所有的消费者不需要协同，按相同规则取得自己要消费的队列即可
                    // 上述说明未考虑除不尽的情况，如13个队列，3个消费者，则第一个消费者消费5个，其它两个消费4个, 详见代码
                    AllocateMessageQueueStrategy strategy = this.allocateMessageQueueStrategy;

                    List<MessageQueue> allocateResult = null;
                    try {
                        allocateResult = strategy.allocate(
                            this.consumerGroup,
                            this.mQClientFactory.getClientId(),
                            mqAll,
                            cidAll);
                    } catch (Throwable e) {
                        log.error("AllocateMessageQueueStrategy.allocate Exception. allocateMessageQueueStrategyName={}", strategy.getName(),
                            e);
                        return;
                    }
                    // 重新将List变为set
                    Set<MessageQueue> allocateResultSet = new HashSet<MessageQueue>();
                    if (allocateResult != null) {
                        allocateResultSet.addAll(allocateResult);
                    }

                    // 跟新processQueueTable
                    boolean changed = this.updateProcessQueueTableInRebalance(topic, allocateResultSet, isOrder);
                    if (changed) {
                        log.info(
                            "rebalanced result changed. allocateMessageQueueStrategyName={}, group={}, topic={}, clientId={}, mqAllSize={}, cidAllSize={}, rebalanceResultSize={}, rebalanceResultSet={}",
                            strategy.getName(), consumerGroup, topic, this.mQClientFactory.getClientId(), mqSet.size(), cidAll.size(),
                            allocateResultSet.size(), allocateResultSet);

                        // 如果有变化，调用子类的messageQueueChanged进行处理
                        // push consumer更新SubscriptionData的version（当前时间），计算pull的threadhold，发送心跳给broker，更新订阅信息
                        this.messageQueueChanged(topic, mqSet, allocateResultSet);
                    }
                }
                break;
            }
            default:
                break;
        }
    }

    // 删除我不再订阅的topic
    private void truncateMessageQueueNotMyTopic() {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();

        for (MessageQueue mq : this.processQueueTable.keySet()) {
            if (!subTable.containsKey(mq.getTopic())) {

                ProcessQueue pq = this.processQueueTable.remove(mq);
                if (pq != null) {
                    pq.setDropped(true); // 设置dropped=true
                    log.info("doRebalance, {}, truncateMessageQueueNotMyTopic remove unnecessary mq, {}", consumerGroup, mq);
                }
            }
        }
    }

    // 使用新的订阅信息更新processQueueTable
    private boolean updateProcessQueueTableInRebalance(final String topic, final Set<MessageQueue> mqSet,
        final boolean isOrder) {
        boolean changed = false;

        // 第一步:先遍历当前processQueueTable， 如果messageQueue不在mqSet中，则表示该messageQeue本消费者不再订阅，删除之
        // 第二步:再遍历mqSet，如果如果messageQueue不在mqSet中不在processQueueTable中，则表示本消费者原先没订阅该messageQeuue，添加之


        // 第一步:遍历当前processQueueTable
        Iterator<Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            MessageQueue mq = next.getKey();
            ProcessQueue pq = next.getValue();

            if (mq.getTopic().equals(topic)) {
                if (!mqSet.contains(mq)) { // mq没有包含在mqSet中，表示该mq不在由本consumer订阅
                    pq.setDropped(true); // 设置dropped=true
                    // 调用子类方法，删除messageQueue
                    if (this.removeUnnecessaryMessageQueue(mq, pq)) {
                        it.remove(); // 将MessageQueue从processQueueTable中删除
                        changed = true; // 任何变化，changed都=true
                        log.info("doRebalance, {}, remove unnecessary mq, {}", consumerGroup, mq);
                    }
                } else if (pq.isPullExpired()) { // 如果messageQueue拉取超时，也从processQueueTable中删除.后续第二步会再加进去
                    switch (this.consumeType()) {
                        case CONSUME_ACTIVELY: // pull consumer不处理
                            break;
                        case CONSUME_PASSIVELY: // push consumer
                            pq.setDropped(true); // 设置dropped=true
                            // 调用子类方法，删除messageQueue
                            if (this.removeUnnecessaryMessageQueue(mq, pq)) {
                                it.remove();// 将MessageQueue从processQueueTable中删除
                                changed = true;
                                log.error("[BUG]doRebalance, {}, remove unnecessary mq, {}, because pull is pause, so try to fixed it",
                                    consumerGroup, mq);
                            }
                            break;
                        default:
                            break;
                    }
                }
            }
        }

        // 第二步:遍历mqSet
        List<PullRequest> pullRequestList = new ArrayList<PullRequest>(); // 新建一个pullRequestList，用于保存新的拉取请求
        for (MessageQueue mq : mqSet) {
            // processQueueTable中不包含该队列，需要加到processQueueTable中
            if (!this.processQueueTable.containsKey(mq)) {
                if (isOrder && !this.lock(mq)) { // 顺序消费，先忽略
                    log.warn("doRebalance, {}, add a new mq failed, {}, because lock failed", consumerGroup, mq);
                    continue;
                }

                // 清除脏数据，应该是防止该mq先被订阅，然后又不订阅，然后再次订阅时，使用第一次订阅的脏数据
                // push consumer的removeDirtyOffset将该mq从offsetStore中删除
                this.removeDirtyOffset(mq);
                ProcessQueue pq = new ProcessQueue(); // new一个ProcessQueue
                // 调用子类方法计算从哪里消费
                // 对pushconsumer，都是先从broker查询相同的consumerGroup的mq的消费进度
                // 如果broker没有，则表示该队列还没有被消费过，根据消费类型做相应处理,具体见相应代码
                long nextOffset = this.computePullFromWhere(mq);
                if (nextOffset >= 0) {
                    // 用原子方法put数据，保证相同mq只有一个，否则会生成多余的PullRequest
                    // 并发时可能有人提前将该mq扔进去了吧
                    ProcessQueue pre = this.processQueueTable.putIfAbsent(mq, pq);
                    if (pre != null) {
                        log.info("doRebalance, {}, mq already exists, {}", consumerGroup, mq);
                    } else {
                        log.info("doRebalance, {}, add a new mq, {}", consumerGroup, mq);
                        PullRequest pullRequest = new PullRequest();
                        pullRequest.setConsumerGroup(consumerGroup);
                        pullRequest.setNextOffset(nextOffset);
                        pullRequest.setMessageQueue(mq);
                        pullRequest.setProcessQueue(pq);
                        pullRequestList.add(pullRequest); // 扔到pullRequestList中
                        changed = true;
                    }
                } else {
                    log.warn("doRebalance, {}, add new mq failed, {}", consumerGroup, mq);
                }
            }
        }

        // 调用子类，调度PullRequestList
        this.dispatchPullRequest(pullRequestList);
        // 返回是否有变化
        return changed;
    }

    public abstract void messageQueueChanged(final String topic, final Set<MessageQueue> mqAll,
        final Set<MessageQueue> mqDivided);

    public abstract boolean removeUnnecessaryMessageQueue(final MessageQueue mq, final ProcessQueue pq);

    public abstract ConsumeType consumeType();

    public abstract void removeDirtyOffset(final MessageQueue mq);

    public abstract long computePullFromWhere(final MessageQueue mq);

    public abstract void dispatchPullRequest(final List<PullRequest> pullRequestList);

    public void removeProcessQueue(final MessageQueue mq) {
        ProcessQueue prev = this.processQueueTable.remove(mq);
        if (prev != null) {
            boolean droped = prev.isDropped();
            prev.setDropped(true);
            this.removeUnnecessaryMessageQueue(mq, prev);
            log.info("Fix Offset, {}, remove unnecessary mq, {} Droped: {}", consumerGroup, mq, droped);
        }
    }

    public ConcurrentMap<MessageQueue, ProcessQueue> getProcessQueueTable() {
        return processQueueTable;
    }

    public ConcurrentMap<String, Set<MessageQueue>> getTopicSubscribeInfoTable() {
        return topicSubscribeInfoTable;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public AllocateMessageQueueStrategy getAllocateMessageQueueStrategy() {
        return allocateMessageQueueStrategy;
    }

    public void setAllocateMessageQueueStrategy(AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
    }

    public MQClientInstance getmQClientFactory() {
        return mQClientFactory;
    }

    public void setmQClientFactory(MQClientInstance mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }

    public void destroy() {
        Iterator<Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            next.getValue().setDropped(true); // dropped=true
        }

        this.processQueueTable.clear();
    }
}
