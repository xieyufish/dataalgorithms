package com.shell.dataalgorithms.mapreduce.chap02;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.shell.dataalgorithms.util.DateUtil;

public class SecondarySortMapper extends Mapper<LongWritable, Text, CompositeKey, NaturalValue> {
	
	private CompositeKey reducerKey = new CompositeKey();
	private NaturalValue reducerValue = new NaturalValue();
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, CompositeKey, NaturalValue>.Context context)
			throws IOException, InterruptedException {
		
		String[] tokens = value.toString().split(",");
		if (tokens.length == 3) {
			Date date = DateUtil.getDate(tokens[1]);
			if (date == null) {
				return;
			}
			long timestamp = date.getTime();
			reducerKey.set(tokens[0], timestamp);
			reducerValue.set(timestamp, Double.parseDouble(tokens[2]));
			
			context.write(reducerKey, reducerValue);
		}
	}
}
