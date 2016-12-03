package com.shell.dataalgorithms.mapreduce.chap07;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.shell.dataalgorithms.util.Combination;

public class MBAMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private static final int DEFAULT_NUMBER_OF_PAIRS = 2;
	private static final Text reducerKey = new Text();
	private static final IntWritable NUMBER_ONE = new IntWritable(1);
	private int numberOfPairs;

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		this.numberOfPairs = context.getConfiguration().getInt("number.of.pairs", DEFAULT_NUMBER_OF_PAIRS);
	}

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {

		List<String> items = convertItemsToList(value.toString());
		if (items == null || items.size() == 0) {
			return;
		}

		generateMapperOutput(numberOfPairs, items, context);
	}

	private List<String> convertItemsToList(String line) {
		String[] tokens = line.trim().split(",");
		List<String> list = new ArrayList<>();
		if (tokens == null || tokens.length == 0) {
			return null;
		}

		for (String token : tokens) {
			if (token == null || token.trim().equals("")) {
				continue;
			}
			list.add(token.trim());
		}

		return list;
	}

	private void generateMapperOutput(int numberOfPairs, List<String> items, Context context)
			throws IOException, InterruptedException {
		List<List<String>> sortedCombinations = Combination.findSortedCombinations(items, numberOfPairs);
		for (List<String> itemList : sortedCombinations) {
			reducerKey.set(itemList.toString());
			context.write(reducerKey, NUMBER_ONE);
		}
	}

}
