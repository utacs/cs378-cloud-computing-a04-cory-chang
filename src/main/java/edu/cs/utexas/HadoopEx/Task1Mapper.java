package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task1Mapper extends Mapper<Object, Text, Text, IntWritable> {

	// Create a counter and initialize with 1
	private final IntWritable counter = new IntWritable(1);
	// Create a hadoop text object to store words
	private Text word = new Text();

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		try {
			// Generate strings from 1 ride
			String[] ride = value.toString().split(",");
			String hour = ride[2].substring(11,13);
			//System.out.println(hour);
			
			// Check if the line is an error based on GPS coordinates (either a 0 or blank)
			if (ride[6].equals("0") || ride[7].equals("0") || ride[8].equals("0")|| ride[9].equals("0")) {
				word.set(hour);
				context.write(word, counter);
			} else if (ride[6].equals("") || ride[7].equals("") || ride[8].equals("")|| ride[9].equals("")) {
				word.set(hour);
				context.write(word, counter);
			}
		} catch (Exception e) {
			System.out.println("Error line: " + value.toString());
		}
		
		
	}
}