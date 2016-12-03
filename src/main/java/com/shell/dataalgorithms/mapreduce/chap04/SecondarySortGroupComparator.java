package com.shell.dataalgorithms.mapreduce.chap04;

import java.io.IOException;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;

import edu.umd.cloud9.io.pair.PairOfStrings;

public class SecondarySortGroupComparator implements RawComparator<PairOfStrings> {

	@Override
	public int compare(PairOfStrings o1, PairOfStrings o2) {
		return o1.getLeftElement().compareTo(o2.getLeftElement());
	}

	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		DataInputBuffer buffer = new DataInputBuffer();
		PairOfStrings a = new PairOfStrings();
		PairOfStrings b = new PairOfStrings();
		
		try {
			buffer.reset(b1, s1, l1);
			a.readFields(buffer);
			buffer.reset(b2, s2, l2);
			b.readFields(buffer);
			
			return compare(a, b);
		} catch (IOException e) {
			return -1;
		}
	}
	

}
