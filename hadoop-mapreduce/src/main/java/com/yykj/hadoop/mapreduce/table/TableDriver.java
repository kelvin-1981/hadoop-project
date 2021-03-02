package com.yykj.hadoop.mapreduce.table;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TableDriver {

	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		args = new String[] { "e:/test/input12", "e:/test/output12" };
		
		// 1获取配置信息，或者job对象实例
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);
		
		// 2指定本程序的jar包所在的本地路径
		job.setJarByClass(TableDriver.class);
		
		// 3指定本业务job要使用的Mapper/Reducer业务类
		job.setMapperClass(TableMapper.class);
		job.setReducerClass(TableReducer.class);
		
		// 4指定Mapper输出数据的kv类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TableBean.class);
		
		// 5指定最终输出的数据的kv类型
		job.setOutputKeyClass(TableBean.class);
		job.setOutputValueClass(NullWritable.class);
		
		// 6指定job的输入原始文件所在目录
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// 7将job中配置的相关参数，以及job所用的java类所在的jar
		boolean result = job.waitForCompletion(true);
		System.out.println(result ? "Success" : "Fail");
	}

}
