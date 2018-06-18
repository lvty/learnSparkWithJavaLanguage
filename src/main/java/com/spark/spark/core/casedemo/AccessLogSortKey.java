package com.spark.spark.core.casedemo;

import scala.math.Ordered;

import java.io.Serializable;

//日志的二次排序key
public class AccessLogSortKey implements Serializable,Ordered<AccessLogSortKey> {

    private long timestamp;
    private long upFlow;
    private long downFlow;
    public AccessLogSortKey(){}

    @Override
    public String toString() {
        return "AccessLogSortKey{" +
                "timestamp=" + timestamp +
                ", upFlow=" + upFlow +
                ", downFlow=" + downFlow +
                '}';
    }

    public AccessLogSortKey(long timestamp, long upFlow, long downFlow) {
        this.timestamp = timestamp;
        this.upFlow = upFlow;
        this.downFlow = downFlow;
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

    @Override
    public int compare(AccessLogSortKey that) {
        if(this.upFlow != that.upFlow ){
            return (int)(this.upFlow - that.upFlow);
        }else if(this.downFlow != that.downFlow){
            return (int)(this.downFlow - that.downFlow);
        }else if(this.timestamp != that.timestamp){
            return (int)(this.timestamp - that.timestamp);
        }
        return 0;
    }

    @Override
    public int compareTo(AccessLogSortKey that) {
        return this.compare(that);
    }

    @Override
    public boolean $greater(AccessLogSortKey that) {
        if(this.upFlow > that.upFlow) return true;
        else if(this.upFlow == that.upFlow && this.downFlow > that.downFlow) return true;
        else if(this.upFlow == that.upFlow && this.downFlow == that.downFlow
                && this.timestamp > that.timestamp) return true;
        return false;
    }

    @Override
    public boolean $greater$eq(AccessLogSortKey that) {
        if($greater(that)) return true;
        else if(this.upFlow == that.upFlow && this.downFlow == that.downFlow
                && this.timestamp == that.timestamp) return true;
        return false;
    }


    @Override
    public boolean $less(AccessLogSortKey that) {
        if(this.upFlow < that.upFlow) return true;
        else if(this.upFlow == that.upFlow && this.downFlow < that.downFlow) return true;
        else if(this.upFlow == that.upFlow && this.downFlow == that.downFlow
                && this.timestamp < that.timestamp) return true;
        return false;
    }



    @Override
    public boolean $less$eq(AccessLogSortKey that) {
        if($less(that)) return true;
        else if(this.upFlow == that.upFlow && this.downFlow == that.downFlow
                && this.timestamp == that.timestamp) return true;
        return false;
    }

}
