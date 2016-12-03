package com.shell.dataalgorithms.mapreduce.chap02;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.shell.dataalgorithms.util.DateUtil;

public class SecondarySortReducer extends Reducer<CompositeKey, NaturalValue, Text, Text> {

	@Override
	protected void reduce(CompositeKey key, Iterable<NaturalValue> values,
			Reducer<CompositeKey, NaturalValue, Text, Text>.Context context) throws IOException, InterruptedException {
		StringBuilder builder = new StringBuilder();
		for (NaturalValue data : values) {
			builder.append("(");
			String dateAsString = DateUtil.getDateAsString(data.getTimestamp());
			double price = data.getPrice();
			builder.append(dateAsString);
			builder.append(",");
			builder.append(price);
			builder.append(")");
		}
		context.write(new Text(key.getStockSymbol()), new Text(builder.toString()));
	}
	
}
