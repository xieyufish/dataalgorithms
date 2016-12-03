package com.shell.dataalgorithms.mapreduce.chap03;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * 本程序完成的任务: 
 * 输入数据格式为:
 *   key(String),value(int)
 * 输出数据为排名前N的输入记录:
 *   value, key
 *  
 * 在这个程序中,主要是要学会输入数据格式的处理(这里map处理的输入格式为Text,IntWritable;对应的输入文件设置成了SequenceFile了)
 * 以及针对topN这种场景如何处理
 * @author Administrator
 *
 */
public class TopNDriver extends Configured implements Tool {

	private static Logger log = Logger.getLogger(TopNDriver.class);

	public static void main(String[] args) throws Exception {

		if (args.length != 3) {
			log.warn("usage TopNDriver <N> <input> <output>");
			System.exit(1);
		}

		log.info("N=" + args[0]);
		log.info("inputDir=" + args[1]);
		log.info("outputDir=" + args[2]);
		int returnStatus = ToolRunner.run(new TopNDriver(), args);
		System.exit(returnStatus);
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance();
		int N = Integer.parseInt(args[0]);
		job.getConfiguration().setInt("N", N);
		job.setJobName("TopNDriver");
		job.setJarByClass(getClass());

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapperClass(TopNMapper.class);
		job.setReducerClass(TopNReducer.class);
		job.setNumReduceTasks(1);

		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
