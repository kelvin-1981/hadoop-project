package com.yykj.hadoop.mapreduce.flowsum;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.Flow;

/**
 * 
 * @author YYKJ
 *
 */
public class FlowSumReduce extends Reducer<Text, FlowBean, Text, FlowBean> {

	TreeMap<FlowBean, String> treeMap = new TreeMap<>(); 

	@Override
	protected void reduce(Text key, Iterable<FlowBean> beans, Context context)
			throws IOException, InterruptedException {
		
		long sum_upFlow = 0;
		long sum_downFlow = 0;
		//long sumFlow = 0;
		
		for(FlowBean bean : beans){
			sum_upFlow += bean.getUpFlow();
			sum_downFlow += bean.getDownFlow();
			//sumFlow += bean.getSumFlow();
		}
		
		FlowBean resBean = new FlowBean(sum_upFlow,sum_downFlow);

		treeMap.put(resBean, key.toString());
		
		if (treeMap.size() > 5) {
			treeMap.remove(treeMap.lastKey());
		} 
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
	
		for (Entry<FlowBean, String> entry : this.treeMap.entrySet()) {
		    context.write(new Text(entry.getValue()), entry.getKey());
		}
	}
}
