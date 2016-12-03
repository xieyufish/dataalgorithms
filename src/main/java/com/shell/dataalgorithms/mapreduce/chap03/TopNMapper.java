package com.shell.dataalgorithms.mapreduce.chap03;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopNMapper extends Mapper<Text, IntWritable, NullWritable, Text> {
	
	private int N = 10;
	private SortedMap<Integer, String> top = new TreeMap<>();
	
	@Override
	protected void map(Text key, IntWritable value, Mapper<Text, IntWritable, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		String keyAsString = key.toString();
		int frequency = value.get();
		String compositeValue = keyAsString + "," + frequency;
		
		top.put(frequency, compositeValue);
		
		if (top.size() > N) {
			top.remove(top.firstKey());
		}
	}
	
	@Override
	protected void setup(Mapper<Text, IntWritable, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		this.N = context.getConfiguration().getInt("N", 10);
	}
	
	@Override
	protected void cleanup(Mapper<Text, IntWritable, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		for (String str : top.values()) {
			context.write(NullWritable.get(), new Text(str));
		}
	}

}
