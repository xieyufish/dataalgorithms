package com.shell.dataalgorithms.mapreduce.chap01;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondarySortMapper extends Mapper<LongWritable, Text, DateTemperaturePair, Text> {
	private Text theTemperature = new Text();
	private DateTemperaturePair pair = new DateTemperaturePair();
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, DateTemperaturePair, Text>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] lineSplits = line.split(",");
		
		String yearMonth = lineSplits[0] + "-" + lineSplits[1];
		String day = lineSplits[2];
		int temperature = Integer.parseInt(lineSplits[3]);
		
		pair.setYearMonth(new Text(yearMonth));
		pair.setDay(new Text(day));
		pair.setTemperature(new IntWritable(temperature));
		theTemperature.set(lineSplits[3]);
		
		context.write(pair, theTemperature);
	}
}
