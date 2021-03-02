package com.yykj.hadoop.mapreduce.order;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroupingComparator extends WritableComparator {

	protected OrderGroupingComparator() {
		super(OrderBean.class,true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// 要求只要id相同，就认为是相同的key
		
		OrderBean aBean = (OrderBean) a;
		OrderBean bBean = (OrderBean) b;
		
		int result;
		if (aBean.getOrderID() > bBean.getOrderID()) {
			result = 1;
		}else if(aBean.getOrderID() < bBean.getOrderID()){
			result = -1;
		}else {
			result = 0;
		}
		
		return result;
	}
	
}
