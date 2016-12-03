package com.shell.dataalgorithms.mapreduce.chap02;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeyComparator extends WritableComparator {
	
	public CompositeKeyComparator() {
		super(CompositeKey.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
//		System.out.println("=====> invoked this method: chap02.CompositeKeyComparator.compare");
		CompositeKey ck1 = (CompositeKey) a;
		CompositeKey ck2 = (CompositeKey) b;
		
		int comparison = ck1.getStockSymbol().compareTo(ck2.getStockSymbol());
		if (comparison == 0) {
			if (ck1.getTimestamp() == ck2.getTimestamp()) {
				return 0;
			} else if (ck1.getTimestamp() < ck2.getTimestamp()) {
				return -1;
			} else {
				return 1;
			}
		} else {
			return comparison;
		}
	}
}
