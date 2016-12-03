package com.shell.dataalgorithms.mapreduce.chap04;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.umd.cloud9.io.pair.PairOfStrings;

/**
 * input:
 *  <transaction_id><TAB><product_id><TAB><user_id><TAB><quantity><TAB><amount>
 * @author Administrator
 *
 */
public class LeftJoinTransactionMapper extends Mapper<LongWritable, Text, PairOfStrings, PairOfStrings> {
	
	PairOfStrings outputKey = new PairOfStrings();
	PairOfStrings outputValue = new PairOfStrings();
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, PairOfStrings, PairOfStrings>.Context context)
			throws IOException, InterruptedException {
		
		String[] tokens = value.toString().split("\t");
		String productId = tokens[1];
		String userId = tokens[2];
		
		outputKey.set(userId, "2");
		outputValue.set("P", productId);
		
		context.write(outputKey, outputValue);
	}

}
