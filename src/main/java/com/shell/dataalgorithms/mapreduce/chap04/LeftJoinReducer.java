package com.shell.dataalgorithms.mapreduce.chap04;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import edu.umd.cloud9.io.pair.PairOfStrings;

public class LeftJoinReducer extends Reducer<PairOfStrings, PairOfStrings, Text, Text> {
	
	
	@Override
	protected void reduce(PairOfStrings key, Iterable<PairOfStrings> values,
			Reducer<PairOfStrings, PairOfStrings, Text, Text>.Context context) throws IOException, InterruptedException {
		
		Text productId = new Text();
		Text locationId = new Text("undefined");
		Iterator<PairOfStrings> iterator = values.iterator();
		if (iterator.hasNext()) {
			PairOfStrings firstPair = iterator.next();
			System.out.println("firstPair=" + firstPair);
			if (firstPair.getLeftElement().equals("L")) {
				locationId.set(firstPair.getRightElement());
			} else {
				context.write(new Text(firstPair.getRightElement()), locationId);
			}
		}
		
		while (iterator.hasNext()) {
			PairOfStrings productPair = iterator.next();
			System.out.println("productPair=" + productPair);
			productId.set(productPair.getRightElement());
			context.write(productId, locationId);
		}
	}

}
