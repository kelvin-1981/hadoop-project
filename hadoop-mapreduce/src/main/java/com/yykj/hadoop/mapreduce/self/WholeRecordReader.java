package com.yykj.hadoop.mapreduce.self;

import java.io.IOException;

import javax.naming.NamingEnumeration;

import org.apache.commons.lang.ObjectUtils.Null;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


/**
 * 
 * @author YYKJ
 *
 */
public class WholeRecordReader extends RecordReader<Text, BytesWritable>{

	FileSplit split = null;
	
	Configuration conf = null;
	
	Text k = new Text();
	BytesWritable v = new BytesWritable();
	
	boolean isProc = true;
	/**
	 * 初始化
	 */
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		this.split = (FileSplit) split; 	
		
		this.conf = context.getConfiguration();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		
		if(!this.isProc){
			return false;
		}
		
		byte[] buf = new byte[(int) split.getLength()];
		
		//1.获取fs文件信息
		Path path = this.split.getPath();
		FileSystem fs = path.getFileSystem(this.conf);
		
		//2.获取输入流
		FSDataInputStream fis = fs.open(path);
		
		//3.复制
		IOUtils.readFully(fis, buf, 0, buf.length);
		
		//4.封装 v
		v.set(buf, 0, buf.length);
		
		//5.封装k
		k.set(path.toString());

		//6.关闭资源
		IOUtils.closeStream(fis);
		
		this.isProc = false;
		
		return true;
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return this.k;
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
		
		return this.v;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}
}
