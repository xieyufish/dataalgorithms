package com.shell.dataalgorithms.spark.chap08;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class FindCommonFriends {
	
	public static void main(String[] args) {
		
		if (args.length != 2) {
            throw new IllegalArgumentException("usage: Argument 1: input dir, Argument 2: output dir");
        }
		
		String inputPath = args[0];
		String outputPath = args[1];
		
		SparkSession sparkSession = SparkSession.builder().appName(FindCommonFriends.class.getSimpleName()).getOrCreate();
		JavaRDD<String> inputRDD = sparkSession.read().textFile(inputPath).javaRDD();
		
		JavaPairRDD<String, String> flatMapRDD = inputRDD.flatMapToPair(new PairFlatMapFunction<String, String, String>() {

			@Override
			public Iterator<Tuple2<String, String>> call(String t) throws Exception {
				List<Tuple2<String, String>> list = new ArrayList<>();
				String[] tokens = t.split(",");
				String friends = getFriends(tokens);
				
				String person = tokens[0];
				
				for (int i = 1; i < tokens.length; i++) {
					String friend = tokens[i];
					String key = buildSortedKey(person, friend);
					list.add(new Tuple2<String, String>(key, friends));
				}
				return list.iterator();
			}
		});
		
		JavaPairRDD<String, Iterable<String>> groupedRDD = flatMapRDD.groupByKey();
		
		JavaPairRDD<String, String> mapRDD = groupedRDD.mapValues(new Function<Iterable<String>, String>() {

			@Override
			public String call(Iterable<String> v1) throws Exception {
				Map<String, Integer> map = new HashMap<>();
				
				Iterator<String> iterator = v1.iterator();
				int numOfValues = 0;
				
				while (iterator.hasNext()) {
					String friendListString = iterator.next();
					
					if (friendListString.equals("")) {
						continue;
					}
					
					String[] friends = friendListString.split(",");
					addFriends(map, friends);
					
					numOfValues++;
				}
				
				List<String> commonFriends = new ArrayList<>();
				for (Map.Entry<String, Integer> entry : map.entrySet()) {
					if (entry.getValue() == numOfValues && numOfValues > 1) {
						commonFriends.add(entry.getKey());
					}
				}
				
				return commonFriends.toString();
			}
			
			void addFriends(Map<String, Integer> map, String[] friends) {
				for (String friend : friends) {
					if (map.containsKey(friend)) {
						map.put(friend, map.get(friend) + 1);
					} else {
						map.put(friend, 1);
					}
				}
			}
			
		});
		
		mapRDD.saveAsTextFile(outputPath);
		sparkSession.stop();
		
		
	}
	
	static String buildSortedKey(String person, String friend) {
		if (person.compareTo(friend) < 0) {
			return person + "," + friend;
		} else {
			return friend + "," + person;
		}
	}
	
	static String getFriends(String[] tokens) {
		if (tokens.length == 2) {
			return "";
		}
		
		StringBuilder stringBuilder = new StringBuilder();
		for (int i = 1; i < tokens.length; i++) {
			stringBuilder.append(tokens[i]);
			if (i < tokens.length - 1) {
				stringBuilder.append(",");
			}
		}
		
		return stringBuilder.toString();
	}
	
}
