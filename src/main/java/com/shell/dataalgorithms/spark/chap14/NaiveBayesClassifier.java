package com.shell.dataalgorithms.spark.chap14;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

import edu.umd.cloud9.io.pair.PairOfStrings;
import scala.Tuple2;

public class NaiveBayesClassifier {

	public static void main(String[] args) {

		if (args.length != 2) {
			System.err.println("Usage: NaiveBayesClassifier <input-data-filename> <NB-PT-path> ");
			System.exit(1);
		}
		
		String inputDataFileName = args[0];
//		String nbProbTablePath = args[1];
		
		SparkSession sparkSession = SparkSession.builder().appName(NaiveBayesClassifier.class.getSimpleName()).getOrCreate();
		
		JavaRDD<String> inputDataRDD = sparkSession.read().textFile(inputDataFileName).javaRDD();
		
		JavaRDD<Tuple2<PairOfStrings, DoubleWritable>> probabilityRDD = sparkSession.sparkContext().sequenceFile("/user/Administrator/dataalgorithms/chap14/naivebayes/probability", PairOfStrings.class, DoubleWritable.class).toJavaRDD();
//		sparkSession.sparkContext().hadoopFile("/user/Administrator/dataalgorithms/chap14/naivebayes/probability", PairOfStrings.class, DoubleWritable.class,SequenceFileInputFormat.class);
//		sparkSession.sparkContext().hadoopFile("", PairOfStrings.class, DoubleWritable.class, SequenceFileInputFormat.class);
		
		JavaPairRDD<Tuple2<String, String>, Double> mapPairRDD = probabilityRDD.mapToPair(new PairFunction<Tuple2<PairOfStrings, DoubleWritable>, Tuple2<String, String>, Double>() {

			@Override
			public Tuple2<Tuple2<String, String>, Double> call(Tuple2<PairOfStrings, DoubleWritable> t)
					throws Exception {
				Tuple2<String, String> tuple = new Tuple2<String, String>(t._1.getLeftElement(), t._1.getRightElement());
				return new Tuple2<Tuple2<String, String>, Double>(tuple, t._2.get());
			}
			
		});
		
		Map<Tuple2<String, String>, Double> classifier = mapPairRDD.collectAsMap();
		
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
		final Broadcast<Map<Tuple2<String, String>, Double>> broadcastClassifier = javaSparkContext.broadcast(classifier);
		
		JavaRDD<String> classificationsRDD = sparkSession.read().textFile("/user/Administrator/dataalgorithms/chap14/naivebayes/classifications").javaRDD();
		List<String> classifications = classificationsRDD.collect();
		final Broadcast<List<String>> broadcastClassifications = javaSparkContext.broadcast(classifications);
		
		JavaPairRDD<String, String> classifedRDD = inputDataRDD.mapToPair(new PairFunction<String, String, String>() {

			@Override
			public Tuple2<String, String> call(String t) throws Exception {
				String[] tokens = t.split(",");
				Map<Tuple2<String, String>, Double> classifier = broadcastClassifier.value();
				
				List<String> classifications = broadcastClassifications.value();
				Double maxProbability = 0.0;
				String selectedClass = null;
				for (String classification : classifications) {
					Tuple2<String, String> key = new Tuple2<String, String>("CLASS", classification);
					Double probability = classifier.get(key);
					
					for (String attributes : tokens) {
						Tuple2<String, String> conditionKey = new Tuple2<String, String>(attributes, classification);
						Double conditionProbability = classifier.get(conditionKey);
						if (conditionProbability == null) {
							probability = 1.0;
						} else {
							probability *= conditionProbability;
						}
					}
					
					if (selectedClass == null) {
						selectedClass = classification;
						maxProbability = probability;
					} else {
						if (probability > maxProbability) {
							selectedClass = classification;
							maxProbability = probability;
						}
					}
				}
				
				
				return new Tuple2<String, String>(t, selectedClass);
			}
			
		});
		
		classifedRDD.saveAsTextFile("/user/Administrator/dataalgorithms/chap14/output");
		
		javaSparkContext.close();
		sparkSession.stop();
		
	}
}
