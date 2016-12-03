package com.shell.dataalgorithms.mapreduce.chap02;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import com.shell.dataalgorithms.util.DateUtil;

public class NaturalValue implements WritableComparable<NaturalValue> {
	
	private long timestamp;
	private double price;
	
	public NaturalValue() {
		
	}
	
	public NaturalValue(long timestamp, double price) {
		this.timestamp = timestamp;
		this.price = price;
	}
	
	public long getTimestamp() {
		return timestamp;
	}

	public double getPrice() {
		return price;
	}
	
	public void set(long timestamp, double price) {
		this.timestamp = timestamp;
		this.price = price;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(timestamp);
		out.writeDouble(price);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.timestamp = in.readLong();
		this.price = in.readDouble();
	}

	@Override
	public int compareTo(NaturalValue o) {
		if (this.timestamp < o.timestamp) {
			return -1;
		} else if (this.timestamp > o.timestamp) {
			return 1;
		}
		return 0;
	}
	
	public static NaturalValue read(DataInput in) throws IOException {
		NaturalValue value = new NaturalValue();
		value.readFields(in);
		return value;
	}
	
	public String getDate() {
		return DateUtil.getDateAsString(this.timestamp);
	}
}
