package com.yykj.hadoop.mapreduce.order;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable>{

	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		
		String[] fields = value.toString().split("\t");
		int orderID = Integer.parseInt(fields[0]);
		double price = Double.parseDouble(fields[2]);
		
		OrderBean bean = new OrderBean(orderID, price);
		System.out.println("***:" + bean.getOrderID());
		
		context.write(bean, NullWritable.get());
	}

}
