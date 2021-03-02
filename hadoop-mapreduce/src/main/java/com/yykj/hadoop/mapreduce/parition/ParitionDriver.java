package com.yykj.hadoop.mapreduce.parition;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ParitionDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		args = new String[]{"e:/test/input6","e:/test/output6"};
		
		//TODO
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		job.setNumReduceTasks(5);
		job.setPartitionerClass(ParitionCls.class);
		
		job.setJarByClass(ParitionDriver.class);
		
		job.setMapperClass(ParitionMapper.class);
		job.setReducerClass(ParitionReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(PhoneBean.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputKeyClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean result = job.waitForCompletion(true);
		System.out.println(result ? "Success" : "Fail");
	}

}
