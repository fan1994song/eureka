package com.netflix.eureka.util.batcher;

/**
 * @author Tomasz Bak
 */
class TaskHolder<ID, T> {

    /**
     * 任务编号
     */
    private final ID id;
    /**
     * 任务
     */
    private final T task;
    /**
     * 过期时间
     */
    private final long expiryTime;
    /**
     * 提交时间戳
     */
    private final long submitTimestamp;

    TaskHolder(ID id, T task, long expiryTime) {
        this.id = id;
        this.expiryTime = expiryTime;
        this.task = task;
        this.submitTimestamp = System.currentTimeMillis();
    }

    public ID getId() {
        return id;
    }

    public T getTask() {
        return task;
    }

    public long getExpiryTime() {
        return expiryTime;
    }

    public long getSubmitTimestamp() {
        return submitTimestamp;
    }
}
