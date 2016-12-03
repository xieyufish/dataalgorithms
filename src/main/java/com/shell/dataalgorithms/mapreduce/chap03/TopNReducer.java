package com.shell.dataalgorithms.mapreduce.chap03;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopNReducer extends Reducer<NullWritable, Text, IntWritable, Text> {
	
	private int N = 10;
	private SortedMap<Integer, String> top = new TreeMap<>();
	
	@Override
	protected void reduce(NullWritable key, Iterable<Text> values,
			Reducer<NullWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
		
		for (Text value : values) {
			String valueAsString = value.toString().trim();
			String[] tokens = valueAsString.split(",");
			String url = tokens[0];
			int frequency = Integer.parseInt(tokens[1]);
			top.put(frequency, url);
			
			if (top.size() > N) {
				top.remove(top.firstKey());
			}
		}
		
		List<Integer> keys = new ArrayList<>(top.keySet());
		for (int i = keys.size() -1; i >= 0; i--) {
			context.write(new IntWritable(keys.get(i)), new Text(top.get(keys.get(i))));
		}
	}
	
	@Override
	protected void setup(Reducer<NullWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		this.N = context.getConfiguration().getInt("N", 10);
	}

}
