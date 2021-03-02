package com.yykj.hadoop.mapreduce.order;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class OrderBean implements WritableComparable<OrderBean> {

	private int orderID;
	
	private double orderPrice;
	
	public int getOrderID() {
		return orderID;
	}

	public void setOrderID(int orderID) {
		this.orderID = orderID;
	}

	public double getOrderPrice() {
		return orderPrice;
	}

	public void setOrderPrice(double orderPrice) {
		this.orderPrice = orderPrice;
	}

	public OrderBean() {
		super();
		// TODO Auto-generated constructor stub
	}

	public OrderBean(int orderID, double orderPrice) {
		super();
		this.orderID = orderID;
		this.orderPrice = orderPrice;
	}

	@Override
	public int compareTo(OrderBean other) {
		if(this.orderID > other.getOrderID()){
			return 1;
		}
		else if (this.orderID < other.getOrderID()){
			return -1;
		}
		else{
			if(this.orderPrice > other.orderPrice){
				return -1;
			}
			else if(this.orderPrice < other.orderPrice){
				return 1;
			}
			else{
				return 0;
			}
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(this.orderID);
		
		out.writeDouble(this.orderPrice);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.orderID = in.readInt();
		
		this.orderPrice = in.readDouble();
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.orderID + "\t" + this.orderPrice;
	}	
	
}
