package com.shell.dataalgorithms.mapreduce.chap01;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DateTemperatureGroupingComparator extends WritableComparator {
	public DateTemperatureGroupingComparator() {
		super(DateTemperaturePair.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		DateTemperaturePair pair = (DateTemperaturePair) a;
		DateTemperaturePair pair2 = (DateTemperaturePair) b;
		return pair.getYearMonth().compareTo(pair2.getYearMonth());
	}
}
