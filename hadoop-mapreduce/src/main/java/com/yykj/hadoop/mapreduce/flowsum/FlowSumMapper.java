package com.yykj.hadoop.mapreduce.flowsum;

import java.io.IOException;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * 
 * @author YYKJ
 *
 */
public class FlowSumMapper extends Mapper<LongWritable, Text, Text, FlowBean>{

	TreeMap<FlowBean, String> treeMap = new TreeMap<>(); 
	
	/**
	 * key:偏移量
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
			
		String line = value.toString();
		String[] fields = line.split("\t");
		System.out.println("***" + fields[0] + " " + fields[1] + " " + fields[2] + " " + fields[3] + " " + fields[4]);
		
		Text tel = new Text(fields[1]);
		long upFlow = Long.parseLong(fields[3]);
		long downFlow = Long.parseLong(fields[4]);
		
		FlowBean bean = new FlowBean(upFlow, downFlow);
		
		treeMap.put(bean, key.toString());
		
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
