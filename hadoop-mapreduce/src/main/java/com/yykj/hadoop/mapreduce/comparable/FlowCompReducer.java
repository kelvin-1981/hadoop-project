package com.yykj.hadoop.mapreduce.comparable;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowCompReducer extends Reducer<FlowBean, Text, Text, FlowBean> {

	@Override
	protected void reduce(FlowBean key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		for (Text tel : values) {
			context.write(tel, key);
		}
		
	}

}
