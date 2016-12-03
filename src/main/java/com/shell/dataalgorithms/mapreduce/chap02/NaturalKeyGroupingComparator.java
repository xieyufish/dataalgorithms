package com.shell.dataalgorithms.mapreduce.chap02;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class NaturalKeyGroupingComparator extends WritableComparator {
	
	public NaturalKeyGroupingComparator() {
		super(CompositeKey.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
//		System.out.println("=====> invoked this method: NaturalKeyGroupingComparator.compare");
		CompositeKey ck1 = (CompositeKey) a;
		CompositeKey ck2 = (CompositeKey) b;
		return ck1.getStockSymbol().compareTo(ck2.getStockSymbol());
	}
}
