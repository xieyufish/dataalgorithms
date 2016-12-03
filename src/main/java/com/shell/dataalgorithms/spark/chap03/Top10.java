package com.shell.dataalgorithms.spark.chap03;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class Top10 {
	
	public static void main(String[] args) {
		
		final int N = Integer.parseInt(args[0]);
		String inputPath = args[1];
//		String outputPath = args[2];
		SparkSession sparkSession = SparkSession.builder().appName(Top10.class.getSimpleName()).getOrCreate();
		JavaRDD<Tuple2<Text, IntWritable>> input = sparkSession.sparkContext().sequenceFile(inputPath, Text.class, IntWritable.class, 3).toJavaRDD();
		
		JavaRDD<Tuple2<Integer, String>> intermediateTopN = input.mapPartitions(new FlatMapFunction<Iterator<Tuple2<Text, IntWritable>>, Tuple2<Integer, String>>() {

			@Override
			public Iterator<Tuple2<Integer, String>> call(Iterator<Tuple2<Text, IntWritable>> t) throws Exception {
				SortedMap<Integer, String> top = new TreeMap<>();
				while (t.hasNext()) {
					Tuple2<Text, IntWritable> record = t.next();
					top.put(record._2.get(), record._1.toString());
					
					if (top.size() > N) {
						top.remove(top.firstKey());
					}
				}
				System.out.println("=========> " + top);
				
				List<Tuple2<Integer, String>> result = new ArrayList<>();
				Iterator<Integer> keySet = top.keySet().iterator();
				while(keySet.hasNext()) {
					Integer key = keySet.next();
					result.add(new Tuple2<Integer, String>(key, top.get(key)));
				}
				
//				Set<Map.Entry<Integer, String>> entrySet = top.entrySet();
//				Object[] objs = entrySet.toArray();
//				for (int i = 0; i < objs.length; i++) {
//					@SuppressWarnings("unchecked")
//					Map.Entry<Integer, String> entry = (Entry<Integer, String>) objs[i];
//					result.add(new Tuple2<Integer, String>(entry.getKey(), entry.getValue()));
//				}
				return result.iterator();
			}
			
		});
		
		List<Tuple2<Integer, String>> allTopN = intermediateTopN.collect();
		SortedMap<Integer, String> finalTopN = new TreeMap<>();
		for (Tuple2<Integer,String> tuple : allTopN) {
			System.out.println("====>" + tuple._1 + "---" + tuple._2);
			finalTopN.put(tuple._1, tuple._2);
			if (finalTopN.size() > N) {
				finalTopN.remove(finalTopN.firstKey());
			}
		}
		
//		JavaRDD<Tuple2<Integer, String>> result = intermediateTopN.sortBy(new Function<Tuple2<Integer, String>, Tuple2<Integer, String>>() {
//
//			@Override
//			public Tuple2<Integer, String> call(Tuple2<Integer, String> v1) throws Exception {
//				// TODO Auto-generated method stub
//				return v1;
//			}
//			
//		}, false, 1);
//		JavaRDD<String> input = sparkSession.read().textFile(inputPath).javaRDD();
//		JavaPairRDD<String, Integer> pairs = input.mapToPair(new PairFunction<String, String, Integer>() {
//
//			@Override
//			public Tuple2<String, Integer> call(String v1) throws Exception {
//				String[] tokens = v1.split(","); // cat7,234
//	            return new Tuple2<String,Integer>(tokens[0], Integer.parseInt(tokens[1]));
//			}
//			
//		});
		
//		JavaRDD<SortedMap<Integer, String>> intermediateTopN = input.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String, Integer>>, SortedMap<Integer, String>>() {
//			
//			@Override
//			public Iterator<SortedMap<Integer, String>> call(Iterator<Tuple2<String, Integer>> t) throws Exception {
//				SortedMap<Integer, String> top = new TreeMap<>();
//				while (t.hasNext()) {
//					Tuple2<String, Integer> record = t.next();
//					top.put(record._2, record._1);
//					
//					if (top.size() > N) {
//						top.remove(top.firstKey());
//					}
//				}
//				
//				System.out.println("=========> " + top);
				
//				List<Tuple2<Integer, String>> result = new ArrayList<>();
//				Set<Map.Entry<Integer, String>> entrySet = top.entrySet();
//				Object[] objs = entrySet.toArray();
//				for (int i = 0; i < objs.length; i++) {
//					@SuppressWarnings("unchecked")
//					Map.Entry<Integer, String> entry = (Entry<Integer, String>) objs[i];
//					result.add(new Tuple2<Integer, String>(entry.getKey(), entry.getValue()));
//				}
				
//				
//				return Collections.singletonList(top).iterator();
//			}
//		});
		
//		List<SortedMap<Integer, String>> allTopN = intermediateTopN.collect();
//		SortedMap<Integer, String> finalTopN = new TreeMap<>();
//		
//		for (SortedMap<Integer, String> map : allTopN) {
//			for (Map.Entry<Integer, String> entry : map.entrySet()) {
//				finalTopN.put(entry.getKey(), entry.getValue());
//				
//				if (finalTopN.size() > N) {
//					finalTopN.remove(finalTopN.firstKey());
//				}
//			}
//		}
//		
		for (Map.Entry<Integer, String> map : finalTopN.entrySet()) {
			System.out.println(map.getKey() + "--" + map.getValue());
		}
//		result.saveAsTextFile(outputPath);
		sparkSession.stop();
	}
}
