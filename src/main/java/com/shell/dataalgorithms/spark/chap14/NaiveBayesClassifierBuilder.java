package com.shell.dataalgorithms.spark.chap14;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.SparkSession;

import edu.umd.cloud9.io.pair.PairOfStrings;
import scala.Tuple2;

/**
 * 构造分类器
 * 
 * @author Administrator
 *
 */
public class NaiveBayesClassifierBuilder {

	public static void main(String[] args) {

		if (args.length < 1) {
			System.err.println("Usage: BuildNaiveBayesClassifier <training-data-filename>");
			System.exit(1);
		}

		String trainingDataFileName = args[0];

		SparkSession sparkSession = SparkSession.builder().appName(NaiveBayesClassifierBuilder.class.getSimpleName())
				.getOrCreate();

		JavaRDD<String> inputRDD = sparkSession.read().textFile(trainingDataFileName).javaRDD();

		long trainingDataSize = inputRDD.count();

		JavaPairRDD<Tuple2<String, String>, Integer> flatMapPairRDD = inputRDD
				.flatMapToPair(new PairFlatMapFunction<String, Tuple2<String, String>, Integer>() {

					@Override
					public Iterator<Tuple2<Tuple2<String, String>, Integer>> call(String t) throws Exception {

						List<Tuple2<Tuple2<String, String>, Integer>> result = new ArrayList<>();

						String[] tokens = t.split(",");
						int classificationIndex = tokens.length - 1;
						String classification = tokens[classificationIndex];

						for (int i = 0; i < classificationIndex; i++) {
							Tuple2<String, String> K = new Tuple2<String, String>(tokens[i], classification);
							result.add(new Tuple2<Tuple2<String, String>, Integer>(K, 1));
						}

						Tuple2<String, String> K = new Tuple2<String, String>("CLASS", classification);
						result.add(new Tuple2<Tuple2<String, String>, Integer>(K, 1));

						return result.iterator();
					}

				});

		JavaPairRDD<Tuple2<String, String>, Integer> reduceByKeyRDD = flatMapPairRDD
				.reduceByKey(new Function2<Integer, Integer, Integer>() {

					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						return v1 + v2;
					}

				});

		Map<Tuple2<String, String>, Integer> collectAsMap = reduceByKeyRDD.collectAsMap();

		Map<Tuple2<String, String>, Double> probabilityMap = new HashMap<>();
		List<String> classifications = new ArrayList<>();

		for (Map.Entry<Tuple2<String, String>, Integer> entry : collectAsMap.entrySet()) {
			Tuple2<String, String> key = entry.getKey();
			String classification = key._2;

			if (key._1.equals("CLASS")) {
				probabilityMap.put(key, (entry.getValue() / (double) trainingDataSize));
				classifications.add(classification);
			} else {
				Tuple2<String, String> k = new Tuple2<String, String>("CLASS", classification);
				Double count = probabilityMap.get(k);
				if (count == null || !(count > 0)) {
					probabilityMap.put(key, 0.0);
				} else {
					probabilityMap.put(key, entry.getValue() / count);
				}
			}
		}

		List<Tuple2<PairOfStrings, DoubleWritable>> list = toWritableList(probabilityMap);
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
		JavaPairRDD<PairOfStrings, DoubleWritable> resultRDD = javaSparkContext.parallelizePairs(list);
		resultRDD.saveAsHadoopFile("/user/Administrator/dataalgorithms/chap14/naivebayes/probability",
				PairOfStrings.class, DoubleWritable.class, SequenceFileOutputFormat.class);
		
		JavaRDD<String> classificationsRDD = javaSparkContext.parallelize(classifications);
		classificationsRDD.saveAsTextFile("/user/Administrator/dataalgorithms/chap14/naivebayes/classifications");
		
		javaSparkContext.close();
		sparkSession.stop();

	}

	static List<Tuple2<PairOfStrings, DoubleWritable>> toWritableList(Map<Tuple2<String, String>, Double> map) {
		List<Tuple2<PairOfStrings, DoubleWritable>> result = new ArrayList<>();

		for (Map.Entry<Tuple2<String, String>, Double> entry : map.entrySet()) {
			PairOfStrings pairOfStrings = new PairOfStrings(entry.getKey()._1, entry.getKey()._2);
			DoubleWritable value = new DoubleWritable(entry.getValue());
			result.add(new Tuple2<PairOfStrings, DoubleWritable>(pairOfStrings, value));
		}
		return result;
	}
}
