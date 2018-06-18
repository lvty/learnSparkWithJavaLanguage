package com.spark.spark.core.casedemo;

import java.io.Serializable;

public class AccessInfo implements Serializable {

    private long timestamp;
    private long upFlow;
    private long downFlow;

    @Override
    public String toString() {
        return "AccessInfo{" +
                "timestamp=" + timestamp +
                ", upFlow=" + upFlow +
                ", downFlow=" + downFlow +
                '}';
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public AccessInfo(long timestamp, long upFlow, long downFlow) {
        this.timestamp = timestamp;
        this.upFlow = upFlow;
        this.downFlow = downFlow;
    }

    public AccessInfo(){}
}
