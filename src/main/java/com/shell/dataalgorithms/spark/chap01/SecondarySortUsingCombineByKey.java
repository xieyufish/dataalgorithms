package com.shell.dataalgorithms.spark.chap01;

import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import com.shell.dataalgorithms.util.DataStructures;

import scala.Tuple2;

public class SecondarySortUsingCombineByKey {
	
	public static void main(String[] args) {
		
		if (args.length < 2) {
			System.err.println("Usage: SecondarySortUsingCombineByKey <input> <output>");
            System.exit(1);
		}
		
		String inputPath = args[0];
		System.out.println("inputPath=" + inputPath);
		String outputPath = args[1];
		System.out.println("outputPath=" + outputPath);
		
		SparkSession sparkSession = SparkSession.builder().appName(SecondarySortUsingCombineByKey.class.getSimpleName()).getOrCreate();
		
		JavaRDD<String> lines = sparkSession.read().textFile(inputPath).javaRDD();
		
		System.out.println("=== DEBUG STEP-4 ===");
		JavaPairRDD<String, Tuple2<Integer, Integer>> pairs = lines.mapToPair(new PairFunction<String, String, Tuple2<Integer, Integer>>() {

			@Override
			public Tuple2<String, Tuple2<Integer, Integer>> call(String t) throws Exception {
				String[] tokens = t.split(",");
				System.out.println(tokens[0] + "," + tokens[1] + "," + tokens[2]);
				Tuple2<Integer, Integer> timeValue = new Tuple2<Integer, Integer>(Integer.parseInt(tokens[1]), Integer.parseInt(tokens[2]));
				return new Tuple2<String, Tuple2<Integer, Integer>>(tokens[0], timeValue);
			}
			
		});
		
		List<Tuple2<String, Tuple2<Integer, Integer>>> output = pairs.collect();
		for (Tuple2<String, Tuple2<Integer, Integer>> t : output) {
			Tuple2<Integer, Integer> timeValue = t._2;
			System.out.println(t._1 + "," + timeValue._1 + "," + timeValue._2);
		}
		
		Function<Tuple2<Integer, Integer>, SortedMap<Integer, Integer>> createCombiner =
				new Function<Tuple2<Integer, Integer>, SortedMap<Integer, Integer>>() {

					@Override
					public SortedMap<Integer, Integer> call(Tuple2<Integer, Integer> v1) throws Exception {
						Integer time = v1._1;
						Integer value = v1._2;
						SortedMap<Integer, Integer> map = new TreeMap<>();
						map.put(time, value);
						return map;
					}
			
		};
		
		Function2<SortedMap<Integer, Integer>, Tuple2<Integer, Integer>, SortedMap<Integer, Integer>> mergeValue =
				new Function2<SortedMap<Integer, Integer>, Tuple2<Integer, Integer>, SortedMap<Integer, Integer>>() {

					@Override
					public SortedMap<Integer, Integer> call(SortedMap<Integer, Integer> v1, Tuple2<Integer, Integer> v2)
							throws Exception {
						Integer time = v2._1;
						Integer value = v2._2;
						v1.put(time, value);
						return v1;
					}
			
		};
		
		Function2<SortedMap<Integer, Integer>, SortedMap<Integer, Integer>, SortedMap<Integer, Integer>> mergeCombiners =
				new Function2<SortedMap<Integer, Integer>, SortedMap<Integer, Integer>, SortedMap<Integer, Integer>>() {

					@Override
					public SortedMap<Integer, Integer> call(SortedMap<Integer, Integer> v1,
							SortedMap<Integer, Integer> v2) throws Exception {
						if (v1.size() < v2.size()) {
							return DataStructures.merge(v1, v2);
						} else {
							return DataStructures.merge(v2, v1);
						}
					}
			
		};
		
		JavaPairRDD<String, SortedMap<Integer, Integer>> combined = pairs.combineByKey(createCombiner, mergeValue, mergeCombiners);
		
		System.out.println("=== DEBUG STEP-6 ===");
		List<Tuple2<String, SortedMap<Integer, Integer>>> output2 = combined.collect();
		for (Tuple2<String, SortedMap<Integer, Integer>> t : output2) {
			String name = t._1;
			SortedMap<Integer, Integer> map = t._2;
			System.out.println(name);
			System.out.println(map);
		}
		
		combined.saveAsTextFile(outputPath);
		
		sparkSession.stop();
		System.exit(0);
	}

}
