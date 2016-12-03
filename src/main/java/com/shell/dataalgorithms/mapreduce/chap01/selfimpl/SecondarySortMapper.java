package com.shell.dataalgorithms.mapreduce.chap01.selfimpl;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondarySortMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] lineSplits = line.split(",");
		
		if (lineSplits.length != 4) {
			return;
		}
		
		Text outputKey = new Text(lineSplits[0] + "-" + lineSplits[1]);
		Text outputValue = new Text(lineSplits[3]);
		
		context.write(outputKey, outputValue);
	}

}
