package com.yykj.hadoop.mapreduce.nl;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author YYKJ
 *
 */
public class NLineMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	/**
	 * key:偏移量
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		// TODO Auto-generated method stub
		String line = value.toString();
		String[] fileds = line.split(" ");
		for(String info : fileds){
			context.write(new Text(info), new IntWritable(1));
		}
	}
}
