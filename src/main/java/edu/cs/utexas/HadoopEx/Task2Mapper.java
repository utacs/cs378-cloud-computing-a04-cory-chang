package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task2Mapper extends Mapper<Object, Text, Text, IntWritable> {

	// Create a counter and initialize with 1
	private final IntWritable counter = new IntWritable(1);
	// Create a hadoop text object to store words
	private Text word = new Text();

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		try {
			// Generate strings from 1 ride
			String[] ride = value.toString().split(",");
			String medallion = ride[0];
			
			// Check if the line is an error based on GPS coordinates (either a 0 or blank)
			for (int i = 6; i < 10; i++) {
				if (ride[i].equals("0")) {
					word.set(medallion);
					context.write(word, counter);
				}
				if (ride[i].equals("")) {
					word.set(medallion);
					context.write(word, counter);
				}
			}
		} catch (Exception e) {
			System.out.println("Error line: " + value.toString());
		}
		
	}
}