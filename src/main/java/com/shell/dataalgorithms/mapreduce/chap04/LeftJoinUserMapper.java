package com.shell.dataalgorithms.mapreduce.chap04;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.umd.cloud9.io.pair.PairOfStrings;

/**
 * input:
 *  <user_id><TAB><location_id>
 * @author Administrator
 *
 */
public class LeftJoinUserMapper extends Mapper<LongWritable, Text, PairOfStrings, PairOfStrings> {
	
	PairOfStrings outputKey = new PairOfStrings();
	PairOfStrings outputValue = new PairOfStrings();
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, PairOfStrings, PairOfStrings>.Context context)
			throws IOException, InterruptedException {
		
		String[] tokens = value.toString().split("\t");
		
		if (tokens.length == 2) {
			outputKey.set(tokens[0], "1");
			outputValue.set("L", tokens[1]);
			context.write(outputKey, outputValue);
		}
		
	}

}
