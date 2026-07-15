package Study.breaker;

import Study.metrics.RpcCallMetrics;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author lzk
 * @date 2026/7/14 19:56
 * @description 实现熔断器接口的类：基于环形数组
 */
public class ResponseTimeCircuitBreaker implements CircuitBreaker {
    // 1. 熔断参数
    private final long breakMs = 10000; // 熔断时间

    private volatile long breakStartTime = 0; // 熔断开始时间

    private AtomicReference<State> stateReference = new AtomicReference<>(State.CLOSE);


    // 2. 滑动窗口参数（即环形数组参数）
    private final long windowDurationMs = 10000; // 滑动窗口总长度（10秒）

    private final long slotMs = 1000; // 每个时间槽的长度（1秒），窗口被分成多个槽位

    private final Slot[] slots = new Slot[(int) (windowDurationMs / slotMs)]; // 环形数组：保存最近10s内，每1s的请求统计，每个槽位表示1s的统计数据

    private volatile int currentIndex = 0; // 当前时间对应的槽位下标

    private volatile long currentTime = System.currentTimeMillis() / slotMs * slotMs; // 当前槽位的起始时间（向下取整到秒）

    private final Lock slideLock = new ReentrantLock(); // 滑动窗口互斥锁，实现线程安全

    // 3. 慢请求参数
    private final long slowRequestMs ; // 慢请求的时间阈值：超过阈值将熔断器打开

    private final double slowRatio; // 慢请求比例阈值

    private final int minRequest = 5; // 最小请求数，小于该请求数就不打开熔断器


    public ResponseTimeCircuitBreaker(double slowRatio, long slowRequestMs) {
        this.slowRequestMs = slowRequestMs;
        this.slowRatio = slowRatio;
        for (int i = 0; i < slots.length; i++) {
            slots[i] = new Slot();
        }
    }

    @Override
    public boolean allowRequest() {
        if (stateReference.get() == State.CLOSE) {
            return true;
        }
        if (stateReference.get() == State.HALF_OPEN) {
            return false;
        }
        // 如果熔断器为打开状态，且熔断时间不满足要求
        if (System.currentTimeMillis() - breakStartTime < breakMs) {
            return false;
        }
        return stateReference.compareAndSet(State.OPEN, State.HALF_OPEN); // 尝试将熔断器的状态修改为打开状态
    }

    @Override
    public void recordRpc(RpcCallMetrics metrics) {
        long now = System.currentTimeMillis();
        slideWindowIfNecessary(now);
        boolean slowRequest = !metrics.isComplete() || metrics.getDuration() > slowRequestMs;
        switch (stateReference.get()) {
            case OPEN -> processOpen(slowRequest);
            case HALF_OPEN -> processHalfOpen(slowRequest);
            case CLOSE -> processClose(slowRequest);
        }
    }

    private void processOpen(boolean slowRequest) {

    }

    private void processHalfOpen(boolean slowRequest) {
        if (!slowRequest) {
            stateReference.compareAndSet(State.HALF_OPEN, State.CLOSE);
            return;
        }
        if (stateReference.compareAndSet(State.HALF_OPEN, State.OPEN)) { // 为什么不进行slot[]计算：因为HALF_OPEN 状态下的请求是“探测请求”，它的结果直接决定状态，不参与滑动窗口的慢请求比例计算
            this.breakStartTime = System.currentTimeMillis();
        }
    }

    private void processClose(boolean slowRequest) {
        if (!slowRequest) {
            slots[currentIndex].requestCount.getAndIncrement();
            return;
        }
        slots[currentIndex].requestCount.getAndIncrement();
        slots[currentIndex].errorRequestCount.getAndIncrement();
        // 统计是否超过慢请求阈值，超过则将熔断器打开
        int totalRequest = 0;
        int totalErrorRequest = 0;
        for (Slot slot : slots) {
            totalRequest += slot.requestCount.get();
            totalErrorRequest += slot.errorRequestCount.get();
        }
        if (totalRequest < minRequest) {
            return;
        }
        double errorRatio = ((double)totalErrorRequest) / totalRequest;
        if (errorRatio > slowRatio && this.stateReference.compareAndSet(State.CLOSE, State.OPEN)) {
            breakStartTime = System.currentTimeMillis();
        }
    }

    // 滑动窗口的置零操作
    private void slideWindowIfNecessary(long now) {
        if (now - currentTime < slotMs) {
            return;
        }
        try {
            slideLock.lock();
            // 第一步：双重检测，是否进入新的槽位
            int diff = (int) ((now - currentTime) / slotMs);
            if (diff <= 0) {
                return;
            }

            // 第二步：滑动窗口置0操作
            int step = Math.min(diff, slots.length); // 为什么min：如果diff更大，为什么不使用diff：因为函数的作用是将经过的槽位置0，因此使用min
            for (int i = 0; i < step; i++) {
                int updateIndex = (currentIndex + i + 1) % slots.length;
                slots[updateIndex].requestCount.set(0);
                slots[updateIndex].errorRequestCount.set(0);
            }
            currentIndex = (diff + currentIndex) % slots.length;
            currentTime = now / slotMs * slotMs;
        } finally {
            slideLock.unlock();
        }
    }


    // 每个槽记录总请求数和异常请求数
    public class Slot {
        AtomicInteger requestCount = new AtomicInteger(0); // 总请求数
        AtomicInteger errorRequestCount = new AtomicInteger(0); // 异常请求数 = 慢请求数 + 错误请求数
    }
}
