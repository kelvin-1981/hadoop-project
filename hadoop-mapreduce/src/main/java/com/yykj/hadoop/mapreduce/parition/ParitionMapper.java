package com.yykj.hadoop.mapreduce.parition;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.yykj.flowsum.FlowBean;

public class ParitionMapper extends Mapper<LongWritable, Text, Text, PhoneBean> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String[] fields = value.toString().split("\t");
		
		Text tel = new Text(fields[1]);
		long upFlow = Long.parseLong(fields[3]);
		long downFlow = Long.parseLong(fields[4]);
		
		PhoneBean bean = new PhoneBean(upFlow, downFlow);
		
		context.write(tel, bean);
	}

	
}
