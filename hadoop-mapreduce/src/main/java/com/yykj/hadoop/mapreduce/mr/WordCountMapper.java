package com.yykj.hadoop.mapreduce.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author YYKJ
 * KEYIN:输入数据key              本例：LongWritable 数据读取偏移量
 * VALUEIN：输入数据value         本例：Text 输入数据
 * KEYOUT：输出数据key类型                本例：Text 输出数据 kelvin,1
 * VALUEOUT：输出数据valuele类型    本例：IntWritable数据读取偏移量
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	/**
	 * key:偏移量
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		//转换数据类型
		String line = value.toString();
		
		//空格切割单词
		String[] words = line.split(" ");
		
		//循环写出
		for(String word : words){
			context.write(new Text(word), new IntWritable(1));
		}
	}
}
