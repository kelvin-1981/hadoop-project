package com.yykj.hadoop.mapreduce.comparable;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowCompMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String[] fields = value.toString().split("\t");
		
		Text tel = new Text(fields[1]);
		long upFlow = Long.parseLong(fields[3]);
		long downFlow = Long.parseLong(fields[4]);
		FlowBean bean = new FlowBean(upFlow,downFlow);
		
		context.write(bean, tel);
	}
}
