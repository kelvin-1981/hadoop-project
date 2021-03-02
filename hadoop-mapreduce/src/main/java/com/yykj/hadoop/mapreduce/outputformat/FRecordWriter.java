package com.yykj.hadoop.mapreduce.outputformat;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


public class FRecordWriter extends RecordWriter<Text, NullWritable> {

	FSDataOutputStream yis;
	FSDataOutputStream ois;
	
	public FRecordWriter(TaskAttemptContext job) {
		// TODO Auto-generated constructor stub
		try {
			
			FileSystem fs = FileSystem.get(job.getConfiguration());
			
			yis = fs.create(new Path("e:/yykj.log"));
			ois = fs.create(new Path("e:/other.log"));
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void write(Text key, NullWritable value) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if(key.toString().contains("yykj")){
			yis.writeBytes(key.toString());
		}else{
			ois.writeBytes(key.toString());
		}
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		IOUtils.closeStream(yis);
		IOUtils.closeStream(ois);
	}

}
