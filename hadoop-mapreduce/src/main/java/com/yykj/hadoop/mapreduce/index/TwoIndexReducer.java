package com.yykj.hadoop.mapreduce.index;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TwoIndexReducer extends Reducer<Text, Text, Text, Text>{

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		StringBuffer sb = new StringBuffer();
		
		for (Text info : values) {
			sb.append(info.toString().replace("\t", "-->") +"\t");
		}
		
		context.write(key, new Text(sb.toString()));
	}	
}
