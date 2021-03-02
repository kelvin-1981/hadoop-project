package com.yykj.hadoop.mapreduce.parition;

import java.awt.RenderingHints.Key;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.yykj.flowsum.FlowBean;

public class ParitionReducer extends Reducer<Text, PhoneBean, Text, PhoneBean> {

	@Override
	protected void reduce(Text key, Iterable<PhoneBean> beans, Context context)
			throws IOException, InterruptedException {
		
		long sum_upFlow = 0;
		long sum_downFlow = 0;
		//long sumFlow = 0;
		
		for(PhoneBean bean : beans){
			sum_upFlow += bean.getUpFlow();
			sum_downFlow += bean.getDownFlow();
		}
		
		PhoneBean resBean = new PhoneBean(sum_upFlow,sum_downFlow);
		context.write(key, resBean);
	}
}
