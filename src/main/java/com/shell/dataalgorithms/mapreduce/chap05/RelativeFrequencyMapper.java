package com.shell.dataalgorithms.mapreduce.chap05;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RelativeFrequencyMapper extends Mapper<LongWritable, Text, PairOfWords, IntWritable> {
	
	private int neighborWindow = 2;
	private static final IntWritable ONE = new IntWritable(1);
	
	@Override
	protected void setup(Mapper<LongWritable, Text, PairOfWords, IntWritable>.Context context)
			throws IOException, InterruptedException {
		this.neighborWindow = context.getConfiguration().getInt("neighbor.window", 2);
	}
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, PairOfWords, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		PairOfWords pair = new PairOfWords();
		IntWritable totalCount = new IntWritable();
		
		String[] tokens = value.toString().split(" ");
		
		if (tokens == null || tokens.length < 2) {
			return;
		}
		
		for (int i = 0; i < tokens.length; i++) {
			tokens[i] = tokens[i].replaceAll("\\W+", "");
			
			if (tokens[i].equals("")) {
				continue;
			}
			
			pair.setWord(tokens[i]);
			
			int start = (i - neighborWindow) < 0 ? 0 : i - neighborWindow;
			int end = (i + neighborWindow >= tokens.length) ? tokens.length - 1 : i + neighborWindow;
			for (int j = start; j <= end; j++) {
				if (j == i) {
					continue;
				}
				
				pair.setNeighbor(tokens[j].replaceAll("\\W+", ""));
				System.out.println("=====>" + pair);
				context.write(pair, ONE);
			}
			
			pair.setNeighbor("*");
			totalCount.set(end - start);
			System.out.println("=======>" + pair);
			context.write(pair, totalCount);
		}
	}

}
