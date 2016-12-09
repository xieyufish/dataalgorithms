package com.shell.dataalgorithms.spark.chap13;

import java.util.Map;
import java.util.SortedMap;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import com.shell.dataalgorithms.util.Util;

import scala.Tuple2;

/**
 * 邻近算法(K最近邻算法):是数据挖掘技术中最简单的分类算法之一.<br>
 * 所谓K最近邻，就是k个最近的邻居的意思，说的是每个样本都可以用它最接近的k个邻居来代表。
 * 
 * @author Administrator
 *
 */
public class KNN {

	public static void main(String[] args) {

		if (args.length < 5) {
			System.err.println("Usage: kNN <k-knn> <d-dimension> <R> <S> <output-path>");
			System.exit(1);
		}
		
		final Integer k = Integer.parseInt(args[0]);  // 计算多少个邻居数
		final Integer d = Integer.parseInt(args[1]);  // 从多少个维度去计算跟邻居的距离
		
		// 待确定分类的数据
		// 输入格式: <recordId>;<属性值1>,<属性值2>
		String datasetR = args[2];
		
		// 已确定分类的数据
		// 输入格式: <id>;<分类>;<属性值1>,<属性值2>
		String datasetS = args[3];
		
		String output = args[4];
		
		SparkSession sparkSession = SparkSession.builder().appName(KNN.class.getSimpleName()).getOrCreate();
		JavaRDD<String> datasetSRDD = sparkSession.read().textFile(datasetS).javaRDD();
		JavaRDD<String> datasetRRDD = sparkSession.read().textFile(datasetR).javaRDD();
		
		JavaPairRDD<String, String> cartesianRDD = datasetRRDD.cartesian(datasetSRDD);
		
		JavaPairRDD<String, Tuple2<Double, String>> mapPairRDD = cartesianRDD.mapToPair(new PairFunction<Tuple2<String, String>, String, Tuple2<Double, String>>() {

			@Override
			public Tuple2<String, Tuple2<Double, String>> call(Tuple2<String, String> t) throws Exception {
				String rRecord = t._1;
				String sRecord = t._2;
				
				String[] rTokens = rRecord.split(";");
				String rRecordId = rTokens[0];
				String r = rTokens[1];
				
				String[] sTokens = sRecord.split(";");
				String sClassificationId = sTokens[1];
				String s = sTokens[2];
				
				double distance = Util.calculateDistance(r, s, d);
				
				Tuple2<Double, String> v = new Tuple2<Double, String>(distance, sClassificationId);
				return new Tuple2<String, Tuple2<Double, String>>(rRecordId, v);
			}
			
		});
		
		JavaPairRDD<String, Iterable<Tuple2<Double, String>>> groupKeyRDD = mapPairRDD.groupByKey();
		
		JavaPairRDD<String, String> resultRDD = groupKeyRDD.mapValues(new Function<Iterable<Tuple2<Double, String>>, String>() {

			@Override
			public String call(Iterable<Tuple2<Double, String>> v1) throws Exception {
				SortedMap<Double, String>  nearestK = Util.findNearestK(v1, k);
				
				Map<String, Integer> majority = Util.buildClassificationCount(nearestK);
				
				String selectedClassification = Util.classifyByMajority(majority);
				return selectedClassification;
			}
			
		});
		
		resultRDD.saveAsTextFile(output);
		sparkSession.stop();
	}
}
