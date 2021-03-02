package com.yykj.hadoop.mapreduce.comparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ParitionCls extends Partitioner<FlowBean, Text>{

	@Override
	public int getPartition(FlowBean key, Text value, int numPartitions) {
		
		//136 137 138 ...
		String head = value.toString().substring(0,3);
		
		int index = 0;
		
		if(head.equals("136")){
			index = 0;
		}
		else if(head.equals("137")){
			index = 1;
		}
		else if(head.equals("138")){
			index = 2;
		}
		else if(head.equals("139")){
			index = 3;
		}
		else{
			index = 4;
		}
		
		return index;
	}

}
