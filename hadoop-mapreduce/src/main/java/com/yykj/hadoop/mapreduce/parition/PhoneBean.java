package com.yykj.hadoop.mapreduce.parition;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class PhoneBean implements Writable {
	
	private long upFlow = 0;
	private long downFlow = 0;
	private long sumFlow = 0;

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


	public long getSumFlow() {
		return sumFlow;
	}


	public void setSumFlow(long sumFlow) {
		this.sumFlow = sumFlow;
	}


	public PhoneBean() {
		super();
	}
	

	public PhoneBean(long upFlow, long downFlow) {
		super();
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.sumFlow = upFlow + downFlow;
	}

	/**
	 * 序列化方法
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeLong(this.upFlow);
		out.writeLong(this.downFlow);
		out.writeLong(this.sumFlow);
	}

	/**
	 * 反序列化方法
	 * 实现原理为队列 ，先进先出
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.upFlow = in.readLong();
		this.downFlow = in.readLong();
		this.sumFlow = in.readLong();
	}


	@Override
	public String toString() {
		return "upFlow=" + upFlow + "\tdownFlow=" + downFlow + ",\tsumFlow=" + sumFlow;
	}
	
}