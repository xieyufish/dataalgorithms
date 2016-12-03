package com.shell.dataalgorithms.mapreduce.chap05;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class RelativeFrequencyCombiner extends Reducer<PairOfWords, IntWritable, PairOfWords, IntWritable> {
	
	@Override
	protected void reduce(PairOfWords key, Iterable<IntWritable> values,
			Reducer<PairOfWords, IntWritable, PairOfWords, IntWritable>.Context context)
			throws IOException, InterruptedException {
		System.out.println("combiner in =======>" + key);
		int partialSum = 0;
		for (IntWritable value : values) {
			partialSum += value.get();
		}
		
		context.write(key, new IntWritable(partialSum));
	}

}
