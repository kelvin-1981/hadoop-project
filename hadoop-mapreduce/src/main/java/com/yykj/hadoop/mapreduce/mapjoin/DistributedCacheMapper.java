package com.yykj.hadoop.mapreduce.mapjoin;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringUtils;

public class DistributedCacheMapper extends Mapper<LongWritable, Text, Text, NullWritable>{

	Map<String,String> pdMap = new HashMap<>();
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		URI uri = context.getCacheFiles()[0];
		String path = uri.getPath().toString();
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));
		
		String line;
		while(StringUtils.isNotEmpty(line = reader.readLine())){
			String[] fields = line.split("\t");
			pdMap.put(fields[0],fields[1]);
		}
		
		System.out.println(pdMap);
		reader.close();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		//1获取一行
		String line=value.toString();
		//2截取
		String[] fields=line.split("\t");
		//3获取产品id
		String pId = fields[1];
		//4获取商品名称
		String pdName = pdMap.get(pId);
		
		System.out.println(line+"\t"+pdName);
		//6写出
		context.write(new Text(line+"\t"+pdName),NullWritable.get());
	}

}
