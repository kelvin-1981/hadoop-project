package com.yykj.hadoop.mapreduce.log;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable>{

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String[] fields = value.toString().split(" ");
		
		String status = fields[1];
		if("200".equals(status)){
			context.getCounter("********map", "true").increment(1);
			context.write(value, NullWritable.get());
		}
		else{
			context.getCounter("********map", "false").increment(1);
		}
	}

}
