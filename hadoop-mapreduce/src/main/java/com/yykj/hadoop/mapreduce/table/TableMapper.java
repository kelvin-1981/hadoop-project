package com.yykj.hadoop.mapreduce.table;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean>{

	private String FileName = ""; 
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		FileSplit inputSplit = (FileSplit) context.getInputSplit();
		this.FileName = inputSplit.getPath().getName();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String line  = value.toString();
		
		String p_id = "";
		
		TableBean bean = null;
		
		if(this.FileName.startsWith("order")){
			String[] fields = value.toString().split("\t");
			String order_id = fields[0];
			p_id = fields[1];
			int amount = Integer.parseInt(fields[2]) ;
			
			bean = new TableBean(order_id, p_id, amount, "", "order");
		}
		else{
			String[] fields = value.toString().split("\t");
			p_id = fields[0];
			String p_name = fields[1];
			
			bean = new TableBean("", p_id, -1, p_name, "pd");
		}
		
		context.write(new Text(p_id), bean);
	}

	
}
