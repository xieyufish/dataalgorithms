package com.shell.dataalgorithms.mapreduce.chap01;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class SecondarySortDriver extends Configured implements Tool {
	private static Logger logger = Logger.getLogger(SecondarySortDriver.class);

	@Override
	public int run(String[] args) throws Exception {
		
		Job job = Job.getInstance();
		job.setJarByClass(getClass());
		job.setJobName(getClass().getSimpleName());
		
		job.setMapperClass(SecondarySortMapper.class);
		job.setMapOutputKeyClass(DateTemperaturePair.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(SecondarySortReducer.class);
		job.setGroupingComparatorClass(DateTemperatureGroupingComparator.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean status = job.waitForCompletion(true);
		logger.info("run(): status=" + status);
		
		return status ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			logger.warn("SecondarySortDriver <input-dir> <output-dir>");
			throw new IllegalArgumentException("SecondarySortDriver <input-dir> <output-dir>");
		}
		
		System.exit(ToolRunner.run(new SecondarySortDriver(), args));
		
	}

}
