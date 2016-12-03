package com.shell.dataalgorithms.mapreduce.chap04;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.umd.cloud9.io.pair.PairOfStrings;

public class LeftJoinDriver {

	public static void main(String[] args) throws Exception {
		Path transactions = new Path(args[0]);  // input
		Path users = new Path(args[1]); // input
		Path output = new Path(args[2]); // output
		
		Job job = Job.getInstance();
		job.setJarByClass(LeftJoinDriver.class);
		job.setJobName(LeftJoinDriver.class.getSimpleName());
		
		job.setPartitionerClass(SecondarySortPartitioner.class);
		
		job.setGroupingComparatorClass(SecondarySortGroupComparator.class);
		
		job.setReducerClass(LeftJoinReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
//		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		MultipleInputs.addInputPath(job, transactions, TextInputFormat.class, LeftJoinTransactionMapper.class);
		MultipleInputs.addInputPath(job, users, TextInputFormat.class, LeftJoinUserMapper.class);
		
		job.setMapOutputKeyClass(PairOfStrings.class);
		job.setMapOutputValueClass(PairOfStrings.class);
		FileOutputFormat.setOutputPath(job, output);
		
		job.waitForCompletion(true);
	}
}
