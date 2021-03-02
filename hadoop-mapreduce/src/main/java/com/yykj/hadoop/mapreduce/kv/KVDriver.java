package com.yykj.hadoop.mapreduce.kv;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;


public class KVDriver {

	/**
	 * 
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		args = new String[]{"e:/test/input3","e:/test/output3"};
		
		Configuration conf = new Configuration();
		conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR," ");
		
		// TODO Auto-generated method stub
		Job job = Job.getInstance(conf);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		job.setJarByClass(KVDriver.class);
		job.setMapperClass(KVMapper.class);
		job.setReducerClass(KVReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean result = job.waitForCompletion(true);
		System.out.println(result ? "Success" : "Fail");
	}

}
