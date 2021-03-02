package com.yykj.hadoop.mapreduce.table;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.locks.Condition;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Waitable;

public class TableBean implements Writable {

	private String order_id;//订单id
	private String p_id;//产品id
	private int amount;//产品数量
	private String pname;//产品名称
	private String flag;//表的标记
	
	
	public String getOrder_id() {
		return order_id;
	}

	public void setOrder_id(String order_id) {
		this.order_id = order_id;
	}

	public String getP_id() {
		return p_id;
	}

	public void setP_id(String p_id) {
		this.p_id = p_id;
	}

	public int getAmount() {
		return amount;
	}

	public void setAmount(int amount) {
		this.amount = amount;
	}

	public String getPname() {
		return pname;
	}

	public void setPname(String pname) {
		this.pname = pname;
	}

	public String getFlag() {
		return flag;
	}

	public void setFlag(String flag) {
		this.flag = flag;
	}

	public TableBean() {
		super();
		// TODO Auto-generated constructor stub
	}
	
	
	
	public TableBean(String order_id, String p_id, int amount, String pname, String flag) {
		super();
		this.order_id = order_id;
		this.p_id = p_id;
		this.amount = amount;
		this.pname = pname;
		this.flag = flag;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(order_id);
		out.writeUTF(p_id);
		out.writeInt(amount);
		out.writeUTF(pname);
		out.writeUTF(flag);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		order_id = in.readUTF();
		p_id = in.readUTF();
		amount = in.readInt();
		pname = in.readUTF();
		flag = in.readUTF();
	}

	@Override
	public String toString() {
		return "order_id=" + order_id + ", p_id=" + p_id + ", amount=" + amount + ", pname=" + pname
				+ ", flag=" + flag;
	}
	
}
