package com.shell.dataalgorithms.spark.chap01;

import org.apache.spark.Partitioner;

import scala.Tuple2;

public class CustomPartitioner extends Partitioner {
	
	private final int numPartitioner;
	
	public CustomPartitioner(int partitions) {
		this.numPartitioner = partitions;
	}

	@Override
	public int getPartition(Object key) {
		if (key == null) {
			return 0;
		} else if (key instanceof Tuple2) {
			@SuppressWarnings("unchecked")
			Tuple2<String, Integer> tuple2 = (Tuple2<String, Integer>) key;
			return Math.abs(tuple2._1.hashCode() % numPartitioner);
		} else {
			return Math.abs(key.hashCode() % numPartitioner);
		}
	}

	@Override
	public int numPartitions() {
		return numPartitioner;
	}
	
	@Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + numPartitioner;
        return result;
    }
	
	@Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        //
        if (obj == null) {
            return false;
        }
        //
        if (!(obj instanceof CustomPartitioner)) {
            return false;
        }
        //
        CustomPartitioner other = (CustomPartitioner) obj;
        if (numPartitioner != other.numPartitioner) {
            return false;
        }
        //
        return true;
    }

}
