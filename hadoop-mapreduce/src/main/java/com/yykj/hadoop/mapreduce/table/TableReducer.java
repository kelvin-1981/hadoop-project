package com.yykj.hadoop.mapreduce.table;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable>{

	@Override
	protected void reduce(Text key, Iterable<TableBean> values,
			Context context) throws IOException, InterruptedException {
	
		TableBean bean = new TableBean();
		
		for (TableBean info : values) {
			if("order".equals(info.getFlag())){
				bean.setOrder_id(info.getOrder_id());
				bean.setP_id(info.getP_id());
				bean.setAmount(info.getAmount());
			}
			else{
				bean.setPname(info.getPname());
			}
		}
		
		context.write(bean, NullWritable.get());
	}

	
}
