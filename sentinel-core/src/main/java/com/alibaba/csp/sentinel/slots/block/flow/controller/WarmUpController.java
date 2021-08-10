/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.csp.sentinel.slots.block.flow.controller;

import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.csp.sentinel.util.TimeUtil;
import com.alibaba.csp.sentinel.node.Node;
import com.alibaba.csp.sentinel.slots.block.flow.TrafficShapingController;

/**
 * <p>
 * The principle idea comes from Guava. However, the calculation of Guava is
 * rate-based, which means that we need to translate rate to QPS.
 * </p>
 *
 * <p>
 * Requests arriving at the pulse may drag down long idle systems even though it
 * has a much larger handling capability in stable period. It usually happens in
 * scenarios that require extra time for initialization, e.g. DB establishes a connection,
 * connects to a remote service, and so on. That’s why we need “warm up”.
 * </p>
 *
 * <p>
 * Sentinel's "warm-up" implementation is based on the Guava's algorithm.
 * However, Guava’s implementation focuses on adjusting the request interval,
 * which is similar to leaky bucket. Sentinel pays more attention to
 * controlling the count of incoming requests per second without calculating its interval,
 * which resembles token bucket algorithm.
 * </p>
 *
 * <p>
 * The remaining tokens in the bucket is used to measure the system utility.
 * Suppose a system can handle b requests per second. Every second b tokens will
 * be added into the bucket until the bucket is full. And when system processes
 * a request, it takes a token from the bucket. The more tokens left in the
 * bucket, the lower the utilization of the system; when the token in the token
 * bucket is above a certain threshold, we call it in a "saturation" state.
 * </p>
 *
 * <p>
 * Base on Guava’s theory, there is a linear equation we can write this in the
 * form y = m * x + b where y (a.k.a y(x)), or qps(q)), is our expected QPS
 * given a saturated period (e.g. 3 minutes in), m is the rate of change from
 * our cold (minimum) rate to our stable (maximum) rate, x (or q) is the
 * occupied token.
 * </p>
 *
 * @author jialiang.linjl
 */
public class WarmUpController implements TrafficShapingController {

    // 阈值
    protected double count;
    // 3
    private int coldFactor;
    // 转折点的令牌数，和 Guava 的 thresholdPermits 一个意思
    // [500]
    protected int warningToken = 0;
    // 最大的令牌数，和 Guava 的 maxPermits 一个意思
    // [1000]
    private int maxToken;
    // 斜线斜率
    // [1/25000]
    protected double slope;

    // 累积的令牌数，和 Guava 的 storedPermits 一个意思
    protected AtomicLong storedTokens = new AtomicLong(0);
    // 最后更新令牌的时间
    protected AtomicLong lastFilledTime = new AtomicLong(0);

    public WarmUpController(double count, int warmUpPeriodInSec, int coldFactor) {
        construct(count, warmUpPeriodInSec, coldFactor);
    }

    public WarmUpController(double count, int warmUpPeriodInSec) {
        construct(count, warmUpPeriodInSec, 3);
    }

    // 下面的构造方法，和 Guava 中是差不多的，只不过 thresholdPermits 和 maxPermits 都换了个名字
    private void construct(double count, int warmUpPeriodInSec, int coldFactor) {

        if (coldFactor <= 1) {
            throw new IllegalArgumentException("Cold factor should be larger than 1");
        }

        this.count = count;

        this.coldFactor = coldFactor;

        // warningToken 和 thresholdPermits 是一样的意思，计算结果其实是一样的
        // thresholdPermits = 0.5 * warmupPeriod / stableInterval.
        // 【warningToken = (10*100)/(3-1) = 500】

        // thresholdPermits = 0.5 * warmupPeriod / stableInterval.
        // warningToken = 100;
        warningToken = (int)(warmUpPeriodInSec * count) / (coldFactor - 1);
        // / maxPermits = thresholdPermits + 2 * warmupPeriod /
        // (stableInterval + coldInterval)
        // maxToken = 200

        // maxToken 和 maxPermits 是一样的意思，计算结果其实是一样的
        // maxPermits = thresholdPermits + 2*warmupPeriod/(stableInterval+coldInterval)
        // 【maxToken = 500 + (2*10*100)/(1.0+3) = 1000】
        maxToken = warningToken + (int)(2 * warmUpPeriodInSec * count / (1.0 + coldFactor));

        // slope
        // slope = (coldIntervalMicros - stableIntervalMicros) / (maxPermits
        // - thresholdPermits);

        // 斜率计算
        // slope
        // slope = (coldIntervalMicros-stableIntervalMicros)/(maxPermits-thresholdPermits);
        // 【slope = (3-1.0) / 100 / (1000-500) = 1/25000】
        slope = (coldFactor - 1.0) / count / (maxToken - warningToken);

    }

    @Override
    public boolean canPass(Node node, int acquireCount) {
        return canPass(node, acquireCount, false);
    }

    @Override
    public boolean canPass(Node node, int acquireCount, boolean prioritized) {
        // Sentinel 的 QPS 统计使用的是滑动窗口

        // 当前时间窗口的 QPS
        long passQps = (long) node.passQps();

        // 这里是上一个时间窗口的 QPS，这里的一个窗口跨度是1秒钟
        long previousQps = (long) node.previousPassQps();
        // 同步。设置 storedTokens 和 lastFilledTime 到正确的值
        syncToken(previousQps);

        // 令牌数超过 warningToken，进入梯形区域
        // 开始计算它的斜率
        // 如果进入了警戒线，开始调整他的qps
        long restToken = storedTokens.get();
        if (restToken >= warningToken) {
            // 这里简单说一句，因为当前的令牌数超过了 warningToken 这个阈值，系统处于需要预热的阶段
            // 通过计算当前获取一个令牌所需时间，计算其倒数即是当前系统的最大 QPS 容量
            long aboveToken = restToken - warningToken;
            // 消耗的速度要比warning快，但是要比慢
            // current interval = restToken*slope+1/count
            // 这里计算警戒 QPS 值，就是当前状态下能达到的最高 QPS。
            // (aboveToken * slope + 1.0 / count) 其实就是在当前状态下获取一个令牌所需要的时间
            double warningQps = Math.nextUp(1.0 / (aboveToken * slope + 1.0 / count));
            // 如果不会超过，那么通过，否则不通过
            if (passQps + acquireCount <= warningQps) {
                return true;
            }
        } else {
            // count 是最高能达到的 QPS
            if (passQps + acquireCount <= count) {
                return true;
            }
        }

        return false;
    }

    protected void syncToken(long passQps) {
        // 下面几行代码，说明在第一次进入新的 1 秒钟的时候，做同步
        // 题外话：Sentinel 默认地，1 秒钟分为 2 个时间窗口，分别 500ms
        long currentTime = TimeUtil.currentTimeMillis();
        currentTime = currentTime - currentTime % 1000;
        long oldLastFillTime = lastFilledTime.get();
        if (currentTime <= oldLastFillTime) {
            return;
        }

        // 令牌数量的旧值
        long oldValue = storedTokens.get();
        // 计算新的令牌数量，往下看
        long newValue = coolDownTokens(currentTime, passQps);

        if (storedTokens.compareAndSet(oldValue, newValue)) {
            // 令牌数量上，减去上一分钟的 QPS，然后设置新值
            long currentValue = storedTokens.addAndGet(0 - passQps);
            if (currentValue < 0) {
                storedTokens.set(0L);
            }
            lastFilledTime.set(currentTime);
        }

    }

    // 更新令牌数
    private long coolDownTokens(long currentTime, long passQps) {
        long oldValue = storedTokens.get();
        long newValue = oldValue;

        // 添加令牌的判断前提条件:
        // 当令牌的消耗程度远远低于警戒线的时候

        // 当前令牌数小于 warningToken，添加令牌
        if (oldValue < warningToken) {
            newValue = (long)(oldValue + (currentTime - lastFilledTime.get()) * count / 1000);
        } else if (oldValue > warningToken) {
            // 当前令牌数量处于梯形阶段，
            // 如果当前通过的 QPS 大于 count/coldFactor，说明系统消耗令牌的速度，大于冷却速度
            //    那么不需要添加令牌，否则需要添加令牌
            if (passQps < (int)count / coldFactor) {
                newValue = (long)(oldValue + (currentTime - lastFilledTime.get()) * count / 1000);
            }
        }
        return Math.min(newValue, maxToken);
    }

}
