package com.yykj.hadoop.mapreduce.kv;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author YYKJ
 *
 */
public class KVMapper extends Mapper<Text, Text, Text, IntWritable> {

	/**
	 * key:按符号分割的第一个单词
	 */
	@Override
	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		
		// TODO 
		context.write(key, new IntWritable(1));
	}
}
