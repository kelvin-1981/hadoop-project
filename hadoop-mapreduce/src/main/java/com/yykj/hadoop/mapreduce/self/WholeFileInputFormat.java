package com.yykj.hadoop.mapreduce.self;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * 
 * Key:文件全名（路径+名称）
 * value：文件内容字节流
 *
 */
public class WholeFileInputFormat extends FileInputFormat<Text, BytesWritable> {
	

	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		// TODO Auto-generated method stub
		return false; 
	}

	@Override
	public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		
		WholeRecordReader recordReader = new WholeRecordReader();
		recordReader.initialize(split, context);
		
		return recordReader;
	}

	
}
