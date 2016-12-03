package com.shell.dataalgorithms.spark.chap09;

import java.util.ArrayList;
import java.util.Arrays;
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

/**
 * 好友推荐
 * 
 * @author Administrator
 *
 */
public class FriendRecommendation {

	public static void main(String[] args) {

		if (args.length < 2) {
			System.err.println("Usage: SparkFriendRecommendation <input-path> <output-path>");
			System.exit(1);
		}
		
		String inputPath = args[0];
		String outputPath = args[1];
		
		SparkSession sparkSession = SparkSession.builder().appName(FriendRecommendation.class.getSimpleName()).getOrCreate();
		JavaRDD<String> inputRDD = sparkSession.read().textFile(inputPath).javaRDD();
		JavaPairRDD<String, Tuple2<String, String>> flatMapPairRDD = inputRDD.flatMapToPair(new PairFlatMapFunction<String, String, Tuple2<String, String>>() {

			@Override
			public Iterator<Tuple2<String, Tuple2<String, String>>> call(String t) throws Exception {
				String[] tokens = t.split("\t");
				String person = tokens[0];
				String friendString = tokens[1];
				String[] friends = friendString.split(",");
				
				List<Tuple2<String, Tuple2<String, String>>> result = new ArrayList<>();
				
				for (String friend : friends) {
					Tuple2<String, String> friendLink = new Tuple2<String, String>(friend, "");
					Tuple2<String, Tuple2<String, String>> directFriend = new Tuple2<String, Tuple2<String, String>>(person, friendLink);
					result.add(directFriend);
				}
				
				for (int i = 0; i < friends.length; i++) {
					for (int j = i + 1; j < friends.length; j++) {
						Tuple2<String, String> friendLink = new Tuple2<String, String>(friends[j], person);
						result.add(new Tuple2<String, Tuple2<String, String>>(friends[i], friendLink));
						
						Tuple2<String, String> friendLink2 = new Tuple2<String, String>(friends[i], person);
						result.add(new Tuple2<String, Tuple2<String, String>>(friends[j], friendLink2));
					}
				}
				
				return result.iterator();
			}
			
		});
		
		List<Tuple2<String, Tuple2<String, String>>>  flatMapCollects = flatMapPairRDD.collect();
		for (Tuple2<String, Tuple2<String, String>> flatMapCollect : flatMapCollects) {
			System.out.println(flatMapCollect._1 + ":::" + flatMapCollect._2._1 + "::::" + flatMapCollect._2._2); 
		}
		
		JavaPairRDD<String, Iterable<Tuple2<String, String>>> groupByKeyRDD = flatMapPairRDD.groupByKey();
		
		List<Tuple2<String, Iterable<Tuple2<String, String>>>>  groupCollects = groupByKeyRDD.collect();
		for (Tuple2<String, Iterable<Tuple2<String, String>>> groupCollect : groupCollects) {
			System.out.println(groupCollect._1);
		}
		
		JavaPairRDD<String, List<String>> resultRDD = groupByKeyRDD.mapValues(new Function<Iterable<Tuple2<String, String>>, List<String>>() {

			@Override
			public List<String> call(Iterable<Tuple2<String, String>> v1) throws Exception {
				Map<String, List<String>> map = new HashMap<String, List<String>>();
				Iterator<Tuple2<String, String>> iterator = v1.iterator();
				while(iterator.hasNext()) {
					Tuple2<String, String> tuple = iterator.next();
					String toUser = tuple._1;
					String mutualFriend = tuple._2;
					boolean alreadyFriend = mutualFriend.equals("");
					
					if (map.containsKey(toUser)) {
						if (alreadyFriend) {
							map.put(toUser, null);
						} else if (map.get(toUser) != null) {
							map.get(toUser).add(mutualFriend);
						}
					} else {
						if (alreadyFriend) {
							map.put(toUser, null);
						} else {
							List<String> list = new ArrayList<String>(Arrays.asList(mutualFriend));
							map.put(toUser, list);
						}
					}
				}
				
				List<String> resultList = new ArrayList<>();
				for (Map.Entry<String, List<String>> entry : map.entrySet()) {
					List<String> list = entry.getValue();
					if (list != null && list.size() > 2) {
						resultList.add(entry.getKey());
					}
				}
				
				return resultList;
			}
			
		});
		
		List<Tuple2<String, List<String>>> resultList = resultRDD.collect();
		for (Tuple2<String, List<String>> result : resultList) {
			List<String> set = result._2;
			StringBuilder stringBuilder = new StringBuilder();
			for (String str : set) {
				stringBuilder.append(str);
				stringBuilder.append(" ");
			}
			System.out.println(result._1 + "推荐好友: " + stringBuilder.toString());
		}
		
		resultRDD.saveAsTextFile(outputPath);
		
		sparkSession.stop();
	}
}
