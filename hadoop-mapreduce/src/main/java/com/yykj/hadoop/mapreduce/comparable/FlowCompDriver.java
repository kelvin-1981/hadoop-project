package com.yykj.hadoop.mapreduce.comparable;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class FlowCompDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		args = new String[]{"e:/test/input8","e:/test/output8"};
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setNumReduceTasks(5);
		job.setPartitionerClass(ParitionCls.class);
		
		job.setJarByClass(FlowCompDriver.class);
		
		job.setMapperClass(FlowCompMapper.class);
		job.setReducerClass(FlowCompReducer.class);

		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean result = job.waitForCompletion(true);
		System.out.println(result ? "Success" : "Fail");
	}

}
