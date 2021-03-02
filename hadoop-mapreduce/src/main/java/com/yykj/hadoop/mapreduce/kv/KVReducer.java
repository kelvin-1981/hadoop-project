package com.yykj.hadoop.mapreduce.kv;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KVReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,Context context) 
			throws IOException, InterruptedException {
		
		// TODO Auto-generated method stub
		int sum = 0;
		
		for(IntWritable info : values){
			sum += info.get();
		}
		
		context.write(key, new IntWritable(sum));
	}

}
