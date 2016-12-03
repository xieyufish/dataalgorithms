package com.shell.dataalgorithms.mapreduce.chap01;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class DateTemperaturePair implements WritableComparable<DateTemperaturePair> {
	private Text yearMonth;
	private Text day;
	private IntWritable temperature;

	public DateTemperaturePair() {
		yearMonth = new Text();
		day = new Text();
		temperature = new IntWritable();
	}

	public DateTemperaturePair(Text yearMonth, Text day, IntWritable temperature) {
		this.yearMonth = yearMonth;
		this.day = day;
		this.temperature = temperature;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		yearMonth.write(out);
		day.write(out);
		temperature.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		yearMonth.readFields(in);
		day.readFields(in);
		temperature.readFields(in);
	}

	public Text getYearMonth() {
		return yearMonth;
	}

	public void setYearMonth(Text yearMonth) {
		this.yearMonth = yearMonth;
	}

	public Text getDay() {
		return day;
	}

	public void setDay(Text day) {
		this.day = day;
	}

	public IntWritable getTemperature() {
		return temperature;
	}

	public void setTemperature(IntWritable temperature) {
		this.temperature = temperature;
	}

	@Override
	public int compareTo(DateTemperaturePair o) {
		System.out.println("==========>invoked this compareTo Method: compareTo");
		int compareValue = this.yearMonth.compareTo(o.yearMonth);
		if (compareValue == 0) {
			compareValue = temperature.compareTo(o.temperature);
		}
		return -1 * compareValue;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((day == null) ? 0 : day.hashCode());
		result = prime * result + ((temperature == null) ? 0 : temperature.hashCode());
		result = prime * result + ((yearMonth == null) ? 0 : yearMonth.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DateTemperaturePair other = (DateTemperaturePair) obj;
		if (day == null) {
			if (other.day != null)
				return false;
		} else if (!day.equals(other.day))
			return false;
		if (temperature == null) {
			if (other.temperature != null)
				return false;
		} else if (!temperature.equals(other.temperature))
			return false;
		if (yearMonth == null) {
			if (other.yearMonth != null)
				return false;
		} else if (!yearMonth.equals(other.yearMonth))
			return false;
		return true;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("DateTemperaturePair{yearMonth=");
		builder.append(yearMonth);
		builder.append(", day=");
		builder.append(day);
		builder.append(", temperature=");
		builder.append(temperature);
		builder.append("}");
		return builder.toString();
	}

}
