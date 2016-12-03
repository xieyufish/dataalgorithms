package com.shell.dataalgorithms.mapreduce.chap05;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RelativeFrequencyDriver extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			ToolRunner.printGenericCommandUsage(System.err);
            System.err.println("usage: <window> <input> <output>");
            System.exit(-1);
        }
		ToolRunner.run(new RelativeFrequencyDriver(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		int neighborWindow = Integer.parseInt(args[0]);
		Path inputPath = new Path(args[1]);
		Path outputPath = new Path(args[2]);
		
		Job job = Job.getInstance();
		job.setJarByClass(getClass());
		job.setJobName(getClass().getSimpleName());
		
		job.getConfiguration().setInt("neighbor.window", neighborWindow);
		
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setMapperClass(RelativeFrequencyMapper.class);
		job.setMapOutputKeyClass(PairOfWords.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setCombinerClass(RelativeFrequencyCombiner.class);
		job.setPartitionerClass(OrderInversionPartitioner.class);
		
		job.setReducerClass(RelativeFrequencyReducer.class);
		job.setOutputKeyClass(PairOfWords.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.waitForCompletion(true);
		
		return 0;
	}

}
