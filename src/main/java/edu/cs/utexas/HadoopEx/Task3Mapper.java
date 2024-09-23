package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task3Mapper extends Mapper<Object, Text, Text, Text> {

	// Create a counter and initialize with 1
	private final IntWritable counter = new IntWritable(1);
	// Create a hadoop text object to store words
	private Text word = new Text();

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {

		try {
			// Generate strings from 1 ride
			String ride = value.toString();
			TaxiData temp = new TaxiData(ride);
			if (temp.isValid()) {
				word.set(temp.getHackLic());
				context.write(word, new Text(temp.getTotalAmount() + "," + temp.getDuration()));
			}
		} catch (Exception e) {
			System.out.println("Error line: " + value.toString());
		}
	}
}