package com.shell.dataalgorithms.spark.chap04;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class SparkLeftOuterJoin {
	
	public static void main(String[] args) {
		
		String transactionInput = args[0];
		String userInput = args[1];
		String output = args[2];
		
		SparkSession sparkSession = SparkSession.builder().appName(SparkLeftOuterJoin.class.getSimpleName()).getOrCreate();
		JavaRDD<String> userInputRDD = sparkSession.read().textFile(userInput).javaRDD();
		JavaRDD<String> transactionInputRDD = sparkSession.read().textFile(transactionInput).javaRDD();
		
		JavaPairRDD<String, Tuple2<String, Integer>> userPairRDD = userInputRDD.mapToPair(new PairFunction<String, String, Tuple2<String, Integer>>() {

			@Override
			public Tuple2<String, Tuple2<String, Integer>> call(String t) throws Exception {
				String[] tokens = t.split("\t");
				Tuple2<String, Integer> location = new Tuple2<String, Integer>("L", Integer.parseInt(tokens[1]));
				return new Tuple2<String, Tuple2<String, Integer>>(tokens[0], location);
			}
			
		});
		
		JavaPairRDD<String, Tuple2<String, Integer>> transactionPairRDD = transactionInputRDD.mapToPair(new PairFunction<String, String, Tuple2<String, Integer>>() {

			@Override
			public Tuple2<String, Tuple2<String, Integer>> call(String t) throws Exception {
				String[] tokens = t.split("\t");
				Tuple2<String, Integer> product = new Tuple2<String, Integer>("P", Integer.parseInt(tokens[1]));
				return new Tuple2<String, Tuple2<String, Integer>>(tokens[2], product);
			}
			
		});
		
		JavaPairRDD<String, Integer> userPair2 = userInputRDD.mapToPair(new PairFunction<String, String, Integer>() {

			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {
				String[] tokens = t.split("\t");
				return new Tuple2<String, Integer>(tokens[0], Integer.parseInt(tokens[1]));
			}
			
		});
		
		JavaPairRDD<String, Integer> transactionPair2 = transactionInputRDD.mapToPair(new PairFunction<String, String, Integer>() {

			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {
				String[] tokens = t.split("\t");
				return new Tuple2<String, Integer>(tokens[2], Integer.parseInt(tokens[1]));
			}
			
		});
		
//		JavaPairRDD<String, Tuple2<Integer, Integer>> joinRDD = transactionPair2.join(userPair2);
		JavaPairRDD<String, Tuple2<Integer, Optional<Integer>>> joinRDD = transactionPair2.leftOuterJoin(userPair2);
		JavaPairRDD<String, String> joinResult = joinRDD.mapValues(new Function<Tuple2<Integer, Optional<Integer>>, String>() {

			@Override
			public String call(Tuple2<Integer, Optional<Integer>> v1) throws Exception {
				Optional<Integer> optional = v1._2;
				if (optional.isPresent()) {
					return v1._1 + ":::" + optional.get();
				} else {
					return v1._1 + ":::-1";
				}
			}
			
		});
		
		List<Tuple2<String, String>> collectJoins = joinResult.collect();
		for (Tuple2<String, String> tuple : collectJoins) {
			System.out.println(tuple._2);
		}
		
		JavaPairRDD<String, Tuple2<String, Integer>> unionRDD = transactionPairRDD.union(userPairRDD);
		JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> groupedRDD = unionRDD.groupByKey();
		JavaPairRDD<Integer, Integer> resultRDD = groupedRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Tuple2<String, Integer>>>, Integer, Integer>(){

			@Override
			public Iterator<Tuple2<Integer, Integer>> call(Tuple2<String, Iterable<Tuple2<String, Integer>>> t)
					throws Exception {
				Iterable<Tuple2<String, Integer>> pairs = t._2;
				Integer location = -1;
				List<Integer> products = new ArrayList<>();
				for (Tuple2<String, Integer> t2 : pairs) {
					if (t2._1.equals("L")) {
						location = t2._2;
					} else {
						products.add(t2._2);
					}
				}
				
				List<Tuple2<Integer, Integer>> kvList = new ArrayList<Tuple2<Integer, Integer>>();
				for (Integer product : products) {
					kvList.add(new Tuple2<Integer, Integer>(product, location));
				}
				return kvList.iterator();
			}
			
		});
		
		List<Tuple2<Integer, Integer>> collect = resultRDD.collect();
		for (Tuple2<Integer, Integer> tuple : collect) {
			System.out.println(tuple._1 + "----" + tuple._2);
		}
		
		resultRDD.saveAsTextFile(output);
		
		sparkSession.stop();
		
	}
}
