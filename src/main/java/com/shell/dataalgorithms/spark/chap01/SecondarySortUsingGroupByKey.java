package com.shell.dataalgorithms.spark.chap01;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import com.google.common.collect.Lists;

import scala.Tuple2;

public class SecondarySortUsingGroupByKey {
	
	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("Usage: SecondarySortUsingCombineByKey <input> <output>");
            System.exit(1);
		}
		
		SparkSession sparkSession = SparkSession.builder().appName(SecondarySortUsingGroupByKey.class.getSimpleName()).getOrCreate();
		
		String inputPath = args[0];
		String outputPath = args[1];
		
		JavaRDD<String> lines = sparkSession.sparkContext().textFile(inputPath, 1).toJavaRDD();
		JavaPairRDD<String, Tuple2<Integer, Integer>> pairs = lines.mapToPair(new PairFunction<String, String, Tuple2<Integer, Integer>>() {

			@Override
			public Tuple2<String, Tuple2<Integer, Integer>> call(String t) throws Exception {
				String[] tokens = t.split(",");
				String key = tokens[0];
				Tuple2<Integer, Integer> value = new Tuple2<Integer, Integer>(Integer.parseInt(tokens[1]), Integer.parseInt(tokens[2]));
				return new Tuple2<String, Tuple2<Integer, Integer>>(key, value);
			}
			
		});
		
		JavaPairRDD<String, Iterable<Tuple2<Integer, Integer>>> groupedPairs = pairs.groupByKey();
		
		JavaPairRDD<String, Iterable<Tuple2<Integer, Integer>>> outputRDD = groupedPairs.mapValues(new Function<Iterable<Tuple2<Integer, Integer>>, Iterable<Tuple2<Integer, Integer>>>() {

			@Override
			public Iterable<Tuple2<Integer, Integer>> call(Iterable<Tuple2<Integer, Integer>> v1) throws Exception {
				List<Tuple2<Integer, Integer>> list = Lists.newArrayList(v1);
				Collections.sort(list, new Comparator<Tuple2<Integer, Integer>>() {

					@Override
					public int compare(Tuple2<Integer, Integer> o1, Tuple2<Integer, Integer> o2) {
						return o1._1.compareTo(o2._1);
					}
				});
				return list;
			}
			
		});
		
		outputRDD.saveAsTextFile(outputPath);
		
		sparkSession.stop();
		
		System.exit(0);
	}
}
