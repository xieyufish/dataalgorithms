package com.shell.dataalgorithms.mapreduce.chap05;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class RelativeFrequencyReducer extends Reducer<PairOfWords, IntWritable, PairOfWords, DoubleWritable> {
	
	private double totalCount = 0;
	private final DoubleWritable relativeCount = new DoubleWritable();
	private String currentWord = "NOT_KONW";
	
	@Override
	protected void reduce(PairOfWords key, Iterable<IntWritable> values,
			Reducer<PairOfWords, IntWritable, PairOfWords, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
//		System.out.println("reducer in ============>" + key + "====>" + Lists.newArrayList(values.iterator()));
		if (key.getNeighbor().equals("*")) {
			if (key.getWord().equals(currentWord)) {
				totalCount += getTotalCount(values);
			} else {
				currentWord = key.getWord();
				totalCount = getTotalCount(values);
			}
		} else {
			int count = getTotalCount(values);
			relativeCount.set((double)count / totalCount);
			System.out.println("reduce totalCount ===============>" + totalCount);
			context.write(key, relativeCount);
		}
		
	}
	
	private int getTotalCount(Iterable<IntWritable> values) {
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		
		return sum;
	}

}
