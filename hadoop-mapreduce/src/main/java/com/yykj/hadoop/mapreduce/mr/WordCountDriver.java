package com.yykj.hadoop.mapreduce.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		args = new String[] { "e:/test/input", "e:/test/output" };
		
		// 1 获取Job对象
		Configuration conf = new Configuration();
		//开启map压缩
		conf.setBoolean("mapreduce.map.output.compress", true);
		//设置压缩的方式
		conf.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);
		
		Job job = Job.getInstance(conf);

		// 2 设置jar存储位置
		job.setJarByClass(WordCountDriver.class);

		// 3 关联Map和Reduce类
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
	
		// 4 设置Mapper阶段输出数据的key和value类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// 5 设置最终数据输出的key和value类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// 6 设置输入路径及输出路径
		FileInputFormat.setInputPaths(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		//设置reduce端输出压缩开启
		FileOutputFormat.setCompressOutput(job,true);
		//设置压缩的方式
		FileOutputFormat.setOutputCompressorClass(job,BZip2Codec.class);
		//FileOutputFormat.setOutputCompressorClass(job,GzipCodec.class);
		//FileOutputFormat.setOutputCompressorClass(job,DefaultCodec.class);
		
		// 7 提交Job
		//job.submit();
		//提交并打印信息
		boolean result = job.waitForCompletion(true);
		
		//打印结果
		System.out.println(result ? "Success" : "Fail");
	}

}
