package com.shell.dataalgorithms.mapreduce.chap09;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FriendRecommendationReducer extends Reducer<Text, MutualFriendModel, Text, Text> {
	
	@Override
	protected void reduce(Text key, Iterable<MutualFriendModel> values,
			Reducer<Text, MutualFriendModel, Text, Text>.Context context) throws IOException, InterruptedException {
		
		Map<String, List<String>> map = new HashMap<>();
		
		for (MutualFriendModel mutualFriend : values) {
			String toUser = mutualFriend.getToUser();
			String mutual = mutualFriend.getMutualFriend();
			boolean alreadyFriend = (mutual.equals(""));
			
			if (map.containsKey(toUser)) {
				if (alreadyFriend) {
					map.put(toUser, null);
				} else if (map.get(toUser) != null) {
					map.get(toUser).add(mutual);
				}
			} else {
				if (alreadyFriend) {
					map.put(toUser, null);
				} else {
					List<String> list = new ArrayList<>();
					list.add(mutual);
					map.put(toUser, list);
				}
			}
			
		}
		
		context.write(key, new Text(map.toString()));

	}
	
}
