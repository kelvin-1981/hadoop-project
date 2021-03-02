package com.yykj.hadoop.mapreduce.index;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TwoIndexDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		args = new String[]{"e:/test/input16","e:/test/output16","e:/test/output17"};
		
		if(!JobOneProcess(args[0],args[1])){
			System.out.println("Job1执行失败！");
		}
		
		boolean result = JobTwoProcess(args[1],args[2]);
		System.out.println(result ? "Success" : "Fail");
	}

	/**
	 * 执行Job1
	 * @param inputFile
	 * @param outputFile
	 * @return
	 * @throws ClassNotFoundException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private static boolean JobTwoProcess(String inputFile,String outputFile) throws ClassNotFoundException, IOException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(TwoIndexDriver.class);
		job.setMapperClass(TwoIndexMapper.class);
		job.setReducerClass(TwoIndexReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(inputFile));
		FileOutputFormat.setOutputPath(job, new Path(outputFile));
		
		return job.waitForCompletion(true);
	}

	/**
	 * 执行Job2
	 * @param inputFile
	 * @param outputFile
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	private static boolean JobOneProcess(String inputFile,String outputFile) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(OneIndexDriver.class);
		job.setMapperClass(OneIndexMapper.class);
		job.setReducerClass(OneIndexReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(inputFile));
		FileOutputFormat.setOutputPath(job, new Path(outputFile));
		
		return job.waitForCompletion(true);
	}
	
	

}
