package com.shell.dataalgorithms.spark.chap01;

import java.util.Comparator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;
/**
 * input:
 * 2000,04,29,-40
 * 2003,01,31,20
 * 
 * output:
 * 2000-04,-40
 * 2003-01,20
 * @author Administrator
 *
 */
public class SecondarySortUsingRepartitionAndSortWithinPartitions {

	public static void main(String[] args) {
		if (args.length < 3) {
			System.err.println("Usage: SecondarySortUsingCombineByKey <input> <output>");
            System.exit(1);
		}
		
		int partitions = Integer.parseInt(args[0]);
		String inputPath = args[1];
		String outputPath = args[2];
		
		SparkSession sparkSession = SparkSession.builder().appName(SecondarySortUsingRepartitionAndSortWithinPartitions.class.getSimpleName()).getOrCreate();
		JavaRDD<String> lines = sparkSession.read().textFile(inputPath).javaRDD();
		JavaPairRDD<Tuple2<String, Integer>, Integer> pairs = lines.mapToPair(new PairFunction<String, Tuple2<String, Integer>, Integer>() {

			@Override
			public Tuple2<Tuple2<String, Integer>, Integer> call(String t) throws Exception {
				String[] tokens = t.split(",");
				Integer value = Integer.parseInt(tokens[3]);
				Tuple2<String, Integer> key = new Tuple2<String, Integer>(tokens[0] + "-" + tokens[1],value);
				return new Tuple2<Tuple2<String, Integer>, Integer>(key, value);
			}
			
		});
		
		JavaPairRDD<Tuple2<String, Integer>, Integer> sorted = pairs.repartitionAndSortWithinPartitions(new CustomPartitioner(partitions), new Comparator<Tuple2<String, Integer>>() {
			
			@Override
			public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
				if (o2._1.compareTo(o1._1) == 0) {
					return o2._2.compareTo(o1._2);
				} else {
					return o2._1.compareTo(o1._1);
				}
			}
		});
		
		JavaPairRDD<String, Integer> result = sorted.mapToPair(new PairFunction<Tuple2<Tuple2<String, Integer>, Integer>, String, Integer>() {

			@Override
			public Tuple2<String, Integer> call(Tuple2<Tuple2<String, Integer>, Integer> t) throws Exception {
				return new Tuple2<String, Integer>(t._1._1, t._2);
			}
			
		});
		
		result.saveAsTextFile(outputPath);
		
		sparkSession.stop();
		
	}
}
