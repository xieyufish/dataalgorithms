package com.shell.dataalgorithms.mapreduce.chap04;

import org.apache.hadoop.mapreduce.Partitioner;

import edu.umd.cloud9.io.pair.PairOfStrings;

public class SecondarySortPartitioner extends Partitioner<PairOfStrings, Object> {

	@Override
	public int getPartition(PairOfStrings key, Object value, int numPartitions) {
		return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}

}
