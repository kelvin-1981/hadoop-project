package com.yykj.hadoop.mapreduce.self;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SquenceFileReducere extends Reducer<Text, BytesWritable, Text, BytesWritable>{

	@Override
	protected void reduce(Text key, Iterable<BytesWritable> values,
			Context context) throws IOException, InterruptedException {
		
		// TODO Auto-generated method stub
		for (BytesWritable info : values) {
			context.write(key, info);
		}
	}

}
