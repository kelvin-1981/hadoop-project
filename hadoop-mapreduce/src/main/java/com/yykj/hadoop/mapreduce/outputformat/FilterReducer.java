package com.yykj.hadoop.mapreduce.outputformat;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FilterReducer extends Reducer<Text, NullWritable, Text, NullWritable>{

	@Override
	protected void reduce(Text key, Iterable<NullWritable> values,
			Context context) throws IOException, InterruptedException {
		
		//防止重复数据
		for (NullWritable info : values) {
			context.write(new Text(key.toString() + "\r\n"), info.get());
		}	
	}
}
