package com.shell.dataalgorithms.mapreduce.chap02;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * input:
 * <stockSymbol>,<Date>,<Price>
 * @author Administrator
 *
 */
public class CompositeKey implements WritableComparable<CompositeKey> {
	
	private String stockSymbol;
	private long timestamp;
	
	public CompositeKey() {
	}
	
	public CompositeKey(String stockSymbol, long timestamp) {
		this.stockSymbol = stockSymbol;
		this.timestamp = timestamp;
	}
	
	public String getStockSymbol() {
		return stockSymbol;
	}

	public long getTimestamp() {
		return timestamp;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(stockSymbol);
		out.writeLong(timestamp);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.stockSymbol = in.readUTF();
		this.timestamp = in.readLong();
	}

	@Override
	public int compareTo(CompositeKey o) {
		System.out.println("=====> invoked this method: CompositeKey.compareTo");
		if (this.stockSymbol.compareTo(o.stockSymbol) != 0) {
			return this.stockSymbol.compareTo(o.stockSymbol);
		} else if (this.timestamp != o.timestamp) {
			return this.timestamp < o.timestamp ? -1 : 1;
		} else {
			return 0;
		}
	}
	
	public void set(String stockSymbol, long timestamp) {
		this.stockSymbol = stockSymbol;
		this.timestamp = timestamp;
	}
	
	public static class CompositeKeyComparator extends WritableComparator {
		public CompositeKeyComparator() {
			super(CompositeKey.class);
		}
		
		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			System.out.println("=====> invoked this method: CompositeKeyComparator.compare");
			return compareBytes(b1, s1, l1, b2, s2, l2);
		}
	}
	
	static {
		WritableComparator.define(CompositeKey.class, new CompositeKeyComparator());
	}
}
