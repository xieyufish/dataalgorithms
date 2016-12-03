package com.shell.dataalgorithms.mapreduce.chap08;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CommonFriendsReducer extends Reducer<Text, Text, Text, Text> {
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		Map<String, Integer> map = new HashMap<String, Integer>();
		Iterator<Text> iterator = values.iterator();
		int numOfValues = 0;
		
		while (iterator.hasNext()) {
			String friends = iterator.next().toString();
			if (friends.equals("")) {
				context.write(key, new Text("[]"));
				return;
			}
			addFriends(map, friends);
			numOfValues++;
		}
		
		List<String> commonFriends = new ArrayList<>();
		for (Map.Entry<String, Integer> entry : map.entrySet()) {
			if (numOfValues > 1 && entry.getValue() == numOfValues) {  // 等于1也是不行的吧,取决于具体的好友规则
				commonFriends.add(entry.getKey());
			}
		}
		
		context.write(key, new Text(commonFriends.toString()));
		
	}
	
	void addFriends(Map<String, Integer> map, String friendsList) {
		String[] friends = friendsList.split(",");
		for (String friend : friends) {
			Integer count = map.get(friend);
			if (count == null) {
				map.put(friend, 1);
			} else {
				map.put(friend, ++count);
			}
		}
	}

}
