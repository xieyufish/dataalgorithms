package com.shell.dataalgorithms.spark.chap07;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.SparkSession;

import com.shell.dataalgorithms.util.Combination;

import scala.Tuple2;

public class FindAssociationRules {
	
	public static void main(String[] args) {
		
		if (args.length != 3) {
			System.out.println("USAGE: [input-path] [output-path] [number-of-pairs]");
			System.exit(-1);
		}
		
		String inputPath = args[0];
		String outputPath = args[1];
		final int numOfPairs = Integer.parseInt(args[2]);
		
		SparkSession sparkSession = SparkSession.builder().appName(FindAssociationRules.class.getSimpleName()).getOrCreate();
		
		JavaRDD<String> inputRDD = sparkSession.read().textFile(inputPath).javaRDD();
		
		JavaPairRDD<String, Integer> flatMapPairRDD = inputRDD.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {

			@Override
			public Iterator<Tuple2<String, Integer>> call(String t) throws Exception {
				List<Tuple2<String, Integer>> list = new ArrayList<>();
				String[] tokens = t.split(",");
				if (tokens == null || tokens.length == 0) {
					return null;
				}
				
				List<String> items = new ArrayList<>();
				for (String token : tokens) {
					if (token != null && token.trim().length() > 0) {
						items.add(token);
					}
				}
				
				List<List<String>> sortedCombinations = Combination.findSortedCombinations(items, numOfPairs);
				for (List<String> itemList : sortedCombinations) {
					list.add(new Tuple2<String, Integer>(itemList.toString(), 1));
				}
				return list.iterator();
			}
			
		});
		
		JavaPairRDD<String, Integer> reduceRDD = flatMapPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
			
		});
		
		List<Tuple2<String, Integer>> resultCollects = reduceRDD.collect();
		for (Tuple2<String, Integer> items : resultCollects) {
			System.out.println(items._1 + "<====>" + items._2);
		}
		
		reduceRDD.saveAsTextFile(outputPath);
		
		sparkSession.stop();
	}
}
