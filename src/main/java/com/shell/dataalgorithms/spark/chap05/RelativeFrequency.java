package com.shell.dataalgorithms.spark.chap05;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class RelativeFrequency {

	public static void main(String[] args) {
		
		if (args.length != 3) {
			System.err.println("usage: <window> <input> <output>");
			System.exit(-1);
		}
		
		final Integer window = Integer.parseInt(args[0]);
		String inputPath = args[1];
		String outputPath = args[2];
		
		SparkSession sparkSession = SparkSession.builder().appName(RelativeFrequency.class.getSimpleName()).getOrCreate();
		JavaRDD<String> inputRDD = sparkSession.read().textFile(inputPath).javaRDD();
		
		JavaPairRDD<Tuple2<String, String>, Integer> flatMapPairRDD = inputRDD.flatMapToPair(new PairFlatMapFunction<String, Tuple2<String, String>, Integer>() {

			@Override
			public Iterator<Tuple2<Tuple2<String, String>, Integer>> call(String t) throws Exception {
				List<Tuple2<Tuple2<String, String>, Integer>> resultList = new ArrayList<>();
				String[] tokens = t.split(" ");
				
				if (tokens == null || tokens.length < 2) {
					return null;
				}
				
				for (int i = 0; i < tokens.length; i++) {
					tokens[i] = tokens[i].replaceAll("\\W+", "");
					
					if(tokens[i].equals("")) {
						continue;
					}
					
					int start = (i - window) < 0 ? 0 : i - window;
					int end = (i + window >= tokens.length) ? tokens.length - 1 : i + window;
					for (int j = start; j <= end; j++) {
						if (j == i) {
							continue;
						}
						Tuple2<String, String> key = new Tuple2<String, String>(tokens[i], tokens[j].replaceAll("\\W+", ""));
						resultList.add(new Tuple2<Tuple2<String, String>, Integer>(key, 1));
					}
					
					Tuple2<String, String> key = new Tuple2<String, String>(tokens[i], "*");
					resultList.add(new Tuple2<Tuple2<String, String>, Integer>(key, end - start));
				}
				return resultList.iterator();
			}
			
		});
		
		JavaPairRDD<Tuple2<String, String>, Integer> reduceByKeyRDD = flatMapPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
			
		});
		
		JavaPairRDD<String, Tuple2<Tuple2<String, String>, Integer>>  mapPairRDD = reduceByKeyRDD.mapToPair(new PairFunction<Tuple2<Tuple2<String, String>, Integer>, String, Tuple2<Tuple2<String, String>, Integer>>() {

			@Override
			public Tuple2<String, Tuple2<Tuple2<String, String>, Integer>> call(
					Tuple2<Tuple2<String, String>, Integer> t) throws Exception {
				return new Tuple2<String, Tuple2<Tuple2<String, String>, Integer>>(t._1._1,t);
			}
			
		});
		
		JavaPairRDD<String, Iterable<Tuple2<Tuple2<String, String>, Integer>>> groupKeyRDD = mapPairRDD.groupByKey();
		JavaPairRDD<Tuple2<String, String>, Double> resultFrequencyRDD = groupKeyRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Tuple2<Tuple2<String, String>, Integer>>>, Tuple2<String, String>, Double>() {

			@Override
			public Iterator<Tuple2<Tuple2<String, String>, Double>> call(
					Tuple2<String, Iterable<Tuple2<Tuple2<String, String>, Integer>>> t) throws Exception {
				List<Tuple2<Tuple2<String, String>, Double>> resultList = new ArrayList<>();
				
				Iterator<Tuple2<Tuple2<String, String>, Integer>> iterator = t._2.iterator();
				int totalCount = 0;
				while (iterator.hasNext()) {
					Tuple2<Tuple2<String, String>, Integer> outTuple = iterator.next();
					Tuple2<String, String> key = outTuple._1;
					if (key._2.equals("*")) {
						totalCount = outTuple._2;
					}
				}
				
				for (Tuple2<Tuple2<String, String>, Integer> tuple : t._2) {
					if (!tuple._1._2.equals("*")) {
						double frequency = (tuple._2) / ((double)totalCount);
						resultList.add(new Tuple2<Tuple2<String, String>, Double>(tuple._1, frequency));
					}
				}
				return resultList.iterator();
			}
			
		});
		
		List<Tuple2<Tuple2<String, String>, Double>> results = resultFrequencyRDD.collect();
		for (Tuple2<Tuple2<String, String>, Double> result : results) {
			System.out.println("(" + result._1._1 + "," + result._1._2 + ")" + "\t" + result._2);
		}
		
		resultFrequencyRDD.saveAsTextFile(outputPath);
		
		sparkSession.stop();
		
		
		/*JavaRDD<Tuple2<Tuple2<String, String>, Integer>> flatMapRDD = inputRDD.flatMap(new FlatMapFunction<String, Tuple2<Tuple2<String, String>, Integer>>() {

			@Override
			public Iterator<Tuple2<Tuple2<String, String>, Integer>> call(String t) throws Exception {
				List<Tuple2<Tuple2<String, String>, Integer>> resultList = new ArrayList<>();
				
				String[] tokens = t.split(" ");
				
				if (tokens == null || tokens.length < 2) {
					return null;
				}
				
				for (int i = 0; i < tokens.length; i++) {
					tokens[i] = tokens[i].replaceAll("\\W+", "");
					
					if(tokens[i].equals("")) {
						continue;
					}
					
					int start = (i - window) < 0 ? 0 : i - window;
					int end = (i + window >= tokens.length) ? tokens.length - 1 : i + window;
					for (int j = start; j <= end; j++) {
						if (j == i) {
							continue;
						}
						Tuple2<String, String> key = new Tuple2<String, String>(tokens[i], tokens[j].replaceAll("\\W+", ""));
						resultList.add(new Tuple2<Tuple2<String, String>, Integer>(key, 1));
					}
					
					Tuple2<String, String> key = new Tuple2<String, String>(tokens[i], "*");
					resultList.add(new Tuple2<Tuple2<String, String>, Integer>(key, start - end));
				}
				return resultList.iterator();
			}
			
		});
		
		inputRDD.mapToPair(new PairFunction<String, String, String>() {

			@Override
			public Tuple2<String, String> call(String t) throws Exception {
				String[] tokens = t.split(" ");
				
				if (tokens == null || tokens.length < 2) {
					return null;
				}
				
				for (int i = 0; i < tokens.length; i++) {
					tokens[i] = tokens[i].replaceAll("\\W+", "");
					
					if(tokens[i].equals("")) {
						continue;
					}
					
					
				}
				return null;
			}
			
		});*/
		
	}
}
