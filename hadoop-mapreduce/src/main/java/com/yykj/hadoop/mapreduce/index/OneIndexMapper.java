package com.yykj.hadoop.mapreduce.index;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class OneIndexMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	private String fileName = "";
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		
		FileSplit split = (FileSplit) context.getInputSplit();
		this.fileName = split.getPath().getName();
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
	
		String[] words = value.toString().split(" ");
		
		for (String info : words) {
			context.write(new Text(info + "--" + this.fileName), new IntWritable(1));
		}
	}
}
