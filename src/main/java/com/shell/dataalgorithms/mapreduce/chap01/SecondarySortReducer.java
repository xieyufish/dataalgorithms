package com.shell.dataalgorithms.mapreduce.chap01;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondarySortReducer extends Reducer<DateTemperaturePair, Text, Text, IntWritable> {
	
	@Override
	protected void reduce(DateTemperaturePair key, Iterable<Text> values,
			Reducer<DateTemperaturePair, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		for (Text value : values) {
			context.write(key.getYearMonth(), new IntWritable(Integer.parseInt(value.toString())));
		}
	}

}
