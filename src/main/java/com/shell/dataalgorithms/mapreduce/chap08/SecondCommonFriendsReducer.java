package com.shell.dataalgorithms.mapreduce.chap08;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondCommonFriendsReducer extends Reducer<Text, Text, Text, Text> {
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		List<String> commonFriends = new ArrayList<>();
		for (Text value : values) {
			commonFriends.add(value.toString());
		}
		
		context.write(key, new Text(commonFriends.toString()));
	}

}
