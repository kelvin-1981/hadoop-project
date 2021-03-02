package com.yykj.hadoop.mapreduce.mapjoin;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DistributedCacheDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		// TODO Auto-generated method stub
		//0根据自己电脑路径重新配置
		args = new String[] { "e:/test/input13", "e:/test/output13" };
		//1获取job信息
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);
		//2设置加载jar包路径
		job.setJarByClass(DistributedCacheDriver.class);
		//3关联map
		job.setMapperClass(DistributedCacheMapper.class);
		//4设置最终输出数据类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		//5设置输入输出路径
		FileInputFormat.setInputPaths(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		//6加载缓存数据
		job.addCacheFile(new URI("file:///e:/test/pd.txt"));
		//7Map端Join的逻辑不需要Reduce阶段，设置reduceTask数量为0
		job.setNumReduceTasks(0);
		//8提交
		boolean result=job.waitForCompletion(true);
		System.out.println(result ? "Success" : "Fail");
	}

}
