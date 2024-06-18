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

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.body.ProcessQueueInfo;
import org.apache.rocketmq.logging.InternalLogger;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * NOTE: ProcessQueue，保存消费的snapshot.
 * Queue consumption snapshot
 */
public class ProcessQueue {
    public final static long REBALANCE_LOCK_MAX_LIVE_TIME =
            Long.parseLong(System.getProperty("rocketmq.client.rebalance.lockMaxLiveTime", "30000"));
    public final static long REBALANCE_LOCK_INTERVAL = Long.parseLong(System.getProperty("rocketmq.client.rebalance.lockInterval", "20000"));
    private final static long PULL_MAX_IDLE_TIME = Long.parseLong(System.getProperty("rocketmq.client.pull.pullMaxIdleTime", "120000"));
    private final InternalLogger log = ClientLogger.getLog();
    private final ReadWriteLock treeMapLock = new ReentrantReadWriteLock();
    private final TreeMap<Long, MessageExt> msgTreeMap = new TreeMap<Long, MessageExt>();
    private final AtomicLong msgCount = new AtomicLong();
    private final AtomicLong msgSize = new AtomicLong();
    private final Lock consumeLock = new ReentrantLock();
    /**
     * A subset of msgTreeMap, will only be used when orderly consume
     */
    private final TreeMap<Long/*offset*/, MessageExt> consumingMsgOrderlyTreeMap = new TreeMap<Long, MessageExt>();
    private final AtomicLong tryUnlockTimes = new AtomicLong(0);
    private volatile long queueOffsetMax = 0L;
    private volatile boolean dropped = false;
    private volatile long lastPullTimestamp = System.currentTimeMillis();
    private volatile long lastConsumeTimestamp = System.currentTimeMillis();
    private volatile boolean locked = false;
    private volatile long lastLockTimestamp = System.currentTimeMillis();
    private volatile boolean consuming = false;
    private volatile long msgAccCnt = 0;

    public boolean isLockExpired() {
        return (System.currentTimeMillis() - this.lastLockTimestamp) > REBALANCE_LOCK_MAX_LIVE_TIME;
    }

    public boolean isPullExpired() {
        return (System.currentTimeMillis() - this.lastPullTimestamp) > PULL_MAX_IDLE_TIME;
    }

    /**
     * 清除过期消息.
     *
     * @param pushConsumer
     */
    public void cleanExpiredMsg(DefaultMQPushConsumer pushConsumer) {
        // 顺序消费的consumer直接返回
        if (pushConsumer.getDefaultMQPushConsumerImpl().isConsumeOrderly()) {
            return;
        }
        // 每次最多查询16次
        int loop = msgTreeMap.size() < 16 ? msgTreeMap.size() : 16;
        for (int i = 0; i < loop; i++) {
            MessageExt msg = null;
            try {
                // 使用读锁，防止此时msgTreeMap被写入
                this.treeMapLock.readLock().lockInterruptibly();
                try {
                    // 获取treeMap的第一个节点，如果消费的持续时间超过consumeTimeout，则该消息消费超时，赋值给msg
                    if (!msgTreeMap.isEmpty()) {
                        String consumeStartTimeStamp = MessageAccessor.getConsumeStartTimeStamp(msgTreeMap.firstEntry().getValue());
                        if (StringUtils.isNotEmpty(consumeStartTimeStamp) && System.currentTimeMillis() - Long.parseLong(consumeStartTimeStamp) > pushConsumer.getConsumeTimeout() * 60 * 1000) {
                            msg = msgTreeMap.firstEntry().getValue();
                        } else {
                            // 如果没超时，后面的也不会超时，直接跳出循环
                            break;
                        }
                    } else {
                        break;
                    }
                } finally {
                    // 快速释放读锁
                    this.treeMapLock.readLock().unlock();
                }
            } catch (InterruptedException e) {
                log.error("getExpiredMsg exception", e);
            }

            try {
                // 将消息扔回broker，延迟消费，delayLevel=3，即10秒钟
                pushConsumer.sendMessageBack(msg, 3);
                log.info("send expire msg back. topic={}, msgId={}, storeHost={}, queueId={}, queueOffset={}", msg.getTopic(), msg.getMsgId(), msg.getStoreHost(), msg.getQueueId(), msg.getQueueOffset());
                try {
                    // 使用写锁，用于将第一个消息删除
                    this.treeMapLock.writeLock().lockInterruptibly();
                    try {
                        // 判断msgTreeMap中第一个消息是不是还是刚才的消息. 如果第一个消息的offset和msg的offset一样，即消息没变
                        // 消息超时被放回队列，但消息已经递交给消费者了，还是有可能消费成功。10秒后取出扔回去的消息，如果没有做消费幂等，就会产生重复消费
                        if (!msgTreeMap.isEmpty() && msg.getQueueOffset() == msgTreeMap.firstKey()) {
                            try {
                                // 删除消息
                                removeMessage(Collections.singletonList(msg));
                            } catch (Exception e) {
                                log.error("send expired msg exception", e);
                            }
                        }
                    } finally {
                        // 释放写锁
                        this.treeMapLock.writeLock().unlock();
                    }
                } catch (InterruptedException e) {
                    log.error("getExpiredMsg exception", e);
                }
            } catch (Exception e) {
                log.error("send expired msg exception", e);
            }
        }
    }

    /**
     * 将消息保存到ProcessQueue.
     * <p>
     * 从broker中查询到消息后，调用该方法将查询到的消息保存到ProcessQueue
     *
     * @param msgs 消费到的消息
     * @return 这些消息是否已经传递给了消费者
     * 这个返回值在PollConsumer中被忽略，在PushConsumer中被传递给了consumeMessageService.submitConsumeRequest,但并未被使用（就是没啥用）
     */
    public boolean putMessage(final List<MessageExt> msgs) {
        boolean dispatchToConsume = false;
        try {
            // 写消息，必须写锁
            this.treeMapLock.writeLock().lockInterruptibly();
            try {
                int validMsgCnt = 0;
                // 遍历消息，将消息保存到treeMap中
                for (MessageExt msg : msgs) {
                    MessageExt old = msgTreeMap.put(msg.getQueueOffset(), msg);
                    // 如果old没有值，做后续操作 //old有值，代表该消息被重复消费?
                    if (null == old) {
                        // 有效消息数自增
                        validMsgCnt++;
                        // 使用单独的变量独立保存最大的offset而不是使用lastKey的offset，因为lastKey的消息在并行环境中可能先被确认消费了
                        // 越到后面，消息的offset越大，没毛病
                        this.queueOffsetMax = msg.getQueueOffset();
                        // 计算缓存的总消息大小
                        msgSize.addAndGet(msg.getBody().length);
                    }
                }
                // msgCount加上有效消息数. unsafe效率肯定不如自增，所以此处在计算validMsgCnt后值unsafe操作一次，增加效率
                // 不过不好说，如果是这原因，msgSize也该按相同的法子处理. 即先用临时变量统计出整个消息的长度，最后再加上去，不知道开发者怎么想的
                // 看了下文件历史，msgSize的处理是后面加上的，后面的开发者应该没有注意这一点
                // 此处干嘛不判断下msgs.isEmpty()呢，如果没消息，一次unsafe操作都不用。如果调用时肯定不empty，后面为啥又判断了呢...
                // FIXME: 后续尝试修改msgSize赋值方法；尝试添加msgs.isEmpty判断
                msgCount.addAndGet(validMsgCnt);

                // this.consuming是一个状态值，目前没看明白是用来干嘛的
                // 目前看到this.consuming在takeMessages且没有消息时被置为false,而takeMessages实在顺序消费的messageService中使用，忽略吧
                if (!msgTreeMap.isEmpty() && !this.consuming) {
                    dispatchToConsume = true;
                    this.consuming = true;
                }

                // 通过消息中PROPERTY_MAX_OFFSET属性，计算了下broker中该queue还有多少消息，即针对queue的消息积压数
                // 很奇怪的方式，不知道是每次返回的最后一个消息里携带该属性，还是每个消息都携带
                // 从PullAPIWrapper.processPullResult可看出，每个消息都会携带。
                // FIXME: 但maxOffset是pullResult里携带的，需后面看到底是这一批里最大的，还是队列最大的
                if (!msgs.isEmpty()) {
                    MessageExt messageExt = msgs.get(msgs.size() - 1);
                    String property = messageExt.getProperty(MessageConst.PROPERTY_MAX_OFFSET);
                    if (property != null) {
                        long accTotal = Long.parseLong(property) - messageExt.getQueueOffset();
                        if (accTotal > 0) {
                            this.msgAccCnt = accTotal;
                        }
                    }
                }
            } finally {
                this.treeMapLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("putMessage exception", e);
        }

        return dispatchToConsume;
    }

    /**
     * 获取现存消息的最大span.
     * <p>
     * 该方法在consumer中被调用，如果span超过配置的最大值，则暂停pull message
     *
     * @return
     */
    public long getMaxSpan() {
        try {
            this.treeMapLock.readLock().lockInterruptibly();
            try {
                if (!this.msgTreeMap.isEmpty()) {
                    return this.msgTreeMap.lastKey() - this.msgTreeMap.firstKey();
                }
            } finally {
                this.treeMapLock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("getMaxSpan exception", e);
        }

        return 0;
    }

    /**
     * 消费完成后，调用该方法将消息从ProcessQueue中删除.
     *
     * @param msgs 删除已消费的消息
     * @return 返回消费的offset. 如果删除后，msgTreeMap没消息了，则返回最大offset+1; 否则返回第一个消息的offset.
     * remove后，会用该offset更新offset store. 如果应用挂掉，该offset就是下次消费的起始offset
     */
    public long removeMessage(final List<MessageExt> msgs) {
        long result = -1;
        final long now = System.currentTimeMillis();
        try {
            // 获取写锁
            this.treeMapLock.writeLock().lockInterruptibly();
            // 为啥这里设置最后消费时间呢 // FIXME: 后面看懂了补充
            this.lastConsumeTimestamp = now;
            try {
                if (!msgTreeMap.isEmpty()) {
                    // result为返回值，此处设置result为queueOffsetMax+1
                    result = this.queueOffsetMax + 1;
                    int removedCnt = 0;
                    for (MessageExt msg : msgs) {
                        // 根据offset删除消息
                        MessageExt prev = msgTreeMap.remove(msg.getQueueOffset());
                        if (prev != null) {
                            removedCnt--;
                            // 减少缓存的消息大小
                            msgSize.addAndGet(-msg.getBody().length);
                        }
                    }
                    // 减少缓存消息个数
                    // FIXME：4.9.8,此处加了删除消息后，判断msgCount==0的操作，即没有消息了，设置msgSize=0。前面控制不准吗?这里使用写锁，貌似也不应该有并发冲突E
                    if (msgCount.addAndGet(removedCnt) == 0) {
                        msgSize.set(0);
                    }

                    if (!msgTreeMap.isEmpty()) {
                        // 删除完后，如果还有消息，则result=第一个未确认消息的offset
                        result = msgTreeMap.firstKey();
                    }
                }
            } finally {
                this.treeMapLock.writeLock().unlock();
            }
        } catch (Throwable t) {
            log.error("removeMessage exception", t);
        }

        return result;
    }

    public TreeMap<Long, MessageExt> getMsgTreeMap() {
        return msgTreeMap;
    }

    public AtomicLong getMsgCount() {
        return msgCount;
    }

    public AtomicLong getMsgSize() {
        return msgSize;
    }

    public boolean isDropped() {
        return dropped;
    }

    public void setDropped(boolean dropped) {
        this.dropped = dropped;
    }

    public boolean isLocked() {
        return locked;
    }

    public void setLocked(boolean locked) {
        this.locked = locked;
    }

    // 这个应该是顺序消费的，暂忽略
    public void rollback() {
        try {
            this.treeMapLock.writeLock().lockInterruptibly();
            try {
                this.msgTreeMap.putAll(this.consumingMsgOrderlyTreeMap);
                this.consumingMsgOrderlyTreeMap.clear();
            } finally {
                this.treeMapLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("rollback exception", e);
        }
    }

    // 这个应该是顺序消费的，暂忽略
    public long commit() {
        try {
            this.treeMapLock.writeLock().lockInterruptibly();
            try {
                Long offset = this.consumingMsgOrderlyTreeMap.lastKey();
                if (msgCount.addAndGet(-this.consumingMsgOrderlyTreeMap.size()) == 0) {
                    msgSize.set(0);
                } else {
                    for (MessageExt msg : this.consumingMsgOrderlyTreeMap.values()) {
                        msgSize.addAndGet(-msg.getBody().length);
                    }
                }
                this.consumingMsgOrderlyTreeMap.clear();
                if (offset != null) {
                    return offset + 1;
                }
            } finally {
                this.treeMapLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("commit exception", e);
        }

        return -1;
    }

    // 这个应该是顺序消费的，暂忽略
    public void makeMessageToConsumeAgain(List<MessageExt> msgs) {
        try {
            this.treeMapLock.writeLock().lockInterruptibly();
            try {
                for (MessageExt msg : msgs) {
                    this.consumingMsgOrderlyTreeMap.remove(msg.getQueueOffset());
                    this.msgTreeMap.put(msg.getQueueOffset(), msg);
                }
            } finally {
                this.treeMapLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("makeMessageToCosumeAgain exception", e);
        }
    }

    // 只在ConsumeMessageOrderlyService中调用，暂忽略.
    public List<MessageExt> takeMessages(final int batchSize) {
        List<MessageExt> result = new ArrayList<MessageExt>(batchSize);
        final long now = System.currentTimeMillis();
        try {
            this.treeMapLock.writeLock().lockInterruptibly();
            this.lastConsumeTimestamp = now;
            try {
                if (!this.msgTreeMap.isEmpty()) {
                    for (int i = 0; i < batchSize; i++) {
                        Map.Entry<Long, MessageExt> entry = this.msgTreeMap.pollFirstEntry();
                        if (entry != null) {
                            result.add(entry.getValue());
                            consumingMsgOrderlyTreeMap.put(entry.getKey(), entry.getValue());
                        } else {
                            break;
                        }
                    }
                }

                if (result.isEmpty()) {
                    consuming = false;
                }
            } finally {
                this.treeMapLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("take Messages exception", e);
        }

        return result;
    }

    // FIXME: 这个目前看到是删除不需要的队列时，会调用该方法，判断是否还有消息,后续看到了修改
    public boolean hasTempMessage() {
        try {
            this.treeMapLock.readLock().lockInterruptibly();
            try {
                return !this.msgTreeMap.isEmpty();
            } finally {
                this.treeMapLock.readLock().unlock();
            }
        } catch (InterruptedException e) {
        }

        return true;
    }

    // FIXME: 清除ProcessQueue，也暂时没看清楚用法
    public void clear() {
        try {
            this.treeMapLock.writeLock().lockInterruptibly();
            try {
                this.msgTreeMap.clear();
                this.consumingMsgOrderlyTreeMap.clear();
                this.msgCount.set(0);
                this.msgSize.set(0);
                this.queueOffsetMax = 0L;
            } finally {
                this.treeMapLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("rollback exception", e);
        }
    }

    public long getLastLockTimestamp() {
        return lastLockTimestamp;
    }

    public void setLastLockTimestamp(long lastLockTimestamp) {
        this.lastLockTimestamp = lastLockTimestamp;
    }

    public Lock getConsumeLock() {
        return consumeLock;
    }

    public long getLastPullTimestamp() {
        return lastPullTimestamp;
    }

    public void setLastPullTimestamp(long lastPullTimestamp) {
        this.lastPullTimestamp = lastPullTimestamp;
    }

    public long getMsgAccCnt() {
        return msgAccCnt;
    }

    public void setMsgAccCnt(long msgAccCnt) {
        this.msgAccCnt = msgAccCnt;
    }

    public long getTryUnlockTimes() {
        return this.tryUnlockTimes.get();
    }

    public void incTryUnlockTimes() {
        this.tryUnlockTimes.incrementAndGet();
    }

    // FIXME: 填充ProcessQueueInfo，貌似是跟broker通讯用的，broker会主动查询这个吗？待确认
    public void fillProcessQueueInfo(final ProcessQueueInfo info) {
        try {
            this.treeMapLock.readLock().lockInterruptibly();

            if (!this.msgTreeMap.isEmpty()) {
                info.setCachedMsgMinOffset(this.msgTreeMap.firstKey());
                info.setCachedMsgMaxOffset(this.msgTreeMap.lastKey());
                info.setCachedMsgCount(this.msgTreeMap.size());
            }
            info.setCachedMsgSizeInMiB((int) (this.msgSize.get() / (1024 * 1024)));

            if (!this.consumingMsgOrderlyTreeMap.isEmpty()) {
                info.setTransactionMsgMinOffset(this.consumingMsgOrderlyTreeMap.firstKey());
                info.setTransactionMsgMaxOffset(this.consumingMsgOrderlyTreeMap.lastKey());
                info.setTransactionMsgCount(this.consumingMsgOrderlyTreeMap.size());
            }

            info.setLocked(this.locked);
            info.setTryUnlockTimes(this.tryUnlockTimes.get());
            info.setLastLockTimestamp(this.lastLockTimestamp);

            info.setDroped(this.dropped);
            info.setLastPullTimestamp(this.lastPullTimestamp);
            info.setLastConsumeTimestamp(this.lastConsumeTimestamp);
        } catch (Exception e) {
        } finally {
            this.treeMapLock.readLock().unlock();
        }
    }

    public long getLastConsumeTimestamp() {
        return lastConsumeTimestamp;
    }

    public void setLastConsumeTimestamp(long lastConsumeTimestamp) {
        this.lastConsumeTimestamp = lastConsumeTimestamp;
    }

}
