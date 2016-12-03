package com.shell.dataalgorithms.mapreduce.chap02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SecondarySortDriver {
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
	    Job job = Job.getInstance();
	    job.setJobName("SecondarySort");
	    job.setJarByClass(SecondarySortDriver.class);

        
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
           System.err.println("Usage: SecondarySortDriver <input> <output>");
           System.exit(1);
        }        
	    
	    job.setMapperClass(SecondarySortMapper.class);
	    job.setReducerClass(SecondarySortReducer.class);
	    
        job.setMapOutputKeyClass(CompositeKey.class);
        job.setMapOutputValueClass(NaturalValue.class);
              
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

//	    job.setPartitionerClass(NaturalKeyPartitioner.class);
//	    job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
	    //job.setSortComparatorClass(CompositeKeyComparator.class);
	    
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);

	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

	    job.waitForCompletion(true);
	}
}
