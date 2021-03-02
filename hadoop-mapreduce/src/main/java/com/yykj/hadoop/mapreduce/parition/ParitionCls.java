package com.yykj.hadoop.mapreduce.parition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ParitionCls extends Partitioner<Text, PhoneBean>{

	@Override
	public int getPartition(Text key, PhoneBean value, int numPartitions) {

		//139 138 137
		String head = key.toString().substring(0, 3);
		
		int parIndex = 0;
		switch (head) {
		case "139":
			parIndex = 0;
			break;
		case "138":
			parIndex = 1;
			break;
		case "137":
			parIndex = 2;
			break;
		case "136":
			parIndex = 3;
			break;
		default:
			parIndex = 4;
			break;
		}
		
		System.out.println("**************:"  + parIndex);
		
		return parIndex;
	}
}
