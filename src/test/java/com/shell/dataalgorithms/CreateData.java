package com.shell.dataalgorithms;

import java.io.StringBufferInputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;

import com.google.common.collect.Lists;

@SuppressWarnings("deprecation")
public class CreateData {
	private static Configuration conf = new Configuration();
	private static URI uri = null;
	
	private static char[] chars = new char[]{
			'a','b','c','d','e','f','g',
			'h','i','j','k','l','m','n',
			'o','p','q','r','s','t',
			'u','v','w','x','y','x'
	}; 
	
	static {
		try {
			uri = new URI("hdfs://master:9000");
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws Exception {
//		chap05();
		chap09();
	}
	
	static FSDataOutputStream createFile(Path path) throws Exception {
		FileContext fileContext = FileContext.getFileContext(uri, conf);
		EnumSet<CreateFlag> createFlag = EnumSet.of(CreateFlag.CREATE);
		FSDataOutputStream out = fileContext.create(path, createFlag);
		return out;
	}
	
	static Set<String> createNoReplicateArray() {
//		String[] arrays = new String[length];
		Random random = new Random();
		Set<String> set = new HashSet<>();
		int length = random.nextInt(chars.length);
		length = length <= 0 ? length + 1 : length; 
		while (set.size() < length) {
			set.add(chars[random.nextInt(chars.length)] + "");
		}
		return set;
	}
	
	public static void chap03() throws Exception {
		Random randomNumberGenerator = new Random();
		
		Path path = new Path("/user/Administrator/dataalgorithms/chap03/sample_1.seq");
		
		String key = new String();
		Integer value = null;
		
		SequenceFile.Writer writer = SequenceFile.createWriter(FileSystem.get(uri, conf), conf, path, key.getClass(), Integer.class);
		
		for (int i = 1; i < 1000; i++) {
			int randomInt = randomNumberGenerator.nextInt(i);
			key = "cat" + i;
			value = randomInt;
			System.out.printf("%s\t%s\n", key, value);
			writer.append(key, value);
		}
		
		IOUtils.closeStream(writer);
	}
	
	public static void chap05() throws Exception {
		Random random = new Random();
		
		Path path = new Path("/user/Administrator/dataalgorithms/chap05/sample_data.txt");
		FileContext fileContext = FileContext.getFileContext(uri, conf);
		EnumSet<CreateFlag> createFlag = EnumSet.of(CreateFlag.CREATE);
		FSDataOutputStream out = fileContext.create(path, createFlag);
		
		String seperator = " ";
		StringBuilder content = new StringBuilder();
		for (int i = 0; i < 10; i++) {
			int lineLength = random.nextInt(100);
			for (int j = 0; j < lineLength; j++) {
				content.append(chars[random.nextInt(chars.length)]);
				content.append(seperator);
			}
			content.append("\n");
		}
		
		IOUtils.copyBytes(new StringBufferInputStream(content.toString()), out, conf, true);
	}
	
	public static void chap08() throws Exception {
		
		Path path = new Path("/user/Administrator/dataalgorithms/chap08/sample_data.txt");
		FSDataOutputStream out = createFile(path);
		
		StringBuilder stringBuilder = new StringBuilder();
		for (char c : chars) {
			Set<String> set = createNoReplicateArray();
			stringBuilder.append(c);
			stringBuilder.append(",");
			set.remove(c + "");
			int i = 0;
			for (String value : set) {
				stringBuilder.append(value);
				i++;
				if (i < set.size()) {
					stringBuilder.append(",");
				}
			}
			stringBuilder.append("\n");
		}
		IOUtils.copyBytes(new StringBufferInputStream(stringBuilder.toString()), out, conf, true);
	}
	
	public static void chap09() throws Exception {
		Path path = new Path("/user/Administrator/dataalgorithms/chap09/sample_data.txt");
		FSDataOutputStream out = createFile(path);
		
		StringBuilder stringBuilder = new StringBuilder();
		for (char c : chars) {
			Set<String> set = createNoReplicateArray();
			stringBuilder.append(c);
			stringBuilder.append("\t");
			set.remove(c + "");
			int i = 0;
			for (String value : set) {
				stringBuilder.append(value);
				i++;
				if (i < set.size()) {
					stringBuilder.append(",");
				}
			}
			stringBuilder.append("\n");
		}
		IOUtils.copyBytes(new StringBufferInputStream(stringBuilder.toString()), out, conf, true);
	}
}
