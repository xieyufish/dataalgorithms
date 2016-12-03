package com.shell.dataalgorithms.mapreduce.chap01.selfimpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondarySortReducer extends Reducer<Text, Text, Text, IntWritable> {
	
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
//		System.out.println(Iterators.toString(values.iterator()));
		List<Integer> valueList = new ArrayList<>();
		for (Text value : values) {
			valueList.add(Integer.parseInt(value.toString()));
		}
		
		Collections.sort(valueList);
		
		for (Integer value : valueList) {
			context.write(key, new IntWritable(value));
		}
	}

}
