package com.shell.dataalgorithms.mapreduce.chap05;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderInversionPartitioner extends Partitioner<PairOfWords, IntWritable> {

	@Override
	public int getPartition(PairOfWords key, IntWritable value, int numPartitions) {
		String leftWord = key.getWord();
		return Math.abs(((int)hash(leftWord)) % numPartitions);
	}
	
	
	private static long hash(String str) {
		long h = 31L;
		int length = str.length();
		for (int i = 0; i < length; i++) {
			h = 31*h + str.charAt(i);
		}
		
		return h;
	}
}
