package com.shell.dataalgorithms.mapreduce.chap09;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FriendRecommendationDriver extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.out.println("args is not correct");
			return;
		}
		
		ToolRunner.run(new FriendRecommendationDriver(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		
		Job job = Job.getInstance();
		job.setJarByClass(getClass());
		job.setJobName(getClass().getSimpleName());
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		job.setMapperClass(FriendRecommendationMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MutualFriendModel.class);
		
		job.setReducerClass(FriendRecommendationReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
		return 0;
	}
	
	

}
