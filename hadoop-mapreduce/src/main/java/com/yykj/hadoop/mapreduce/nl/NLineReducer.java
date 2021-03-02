package com.yykj.hadoop.mapreduce.nl;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author YYKJ
 *
 */
public class NLineReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,Context context) 
			throws IOException, InterruptedException {
		
		int sum = 0;
		for(IntWritable info : values){
			sum += info.get();
		}
		
		context.write(key, new IntWritable(sum));
	}

	
}
