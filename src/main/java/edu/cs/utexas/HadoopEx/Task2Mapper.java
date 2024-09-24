package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.StringTokenizer;

import javax.security.auth.callback.TextInputCallback;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task2Mapper extends Mapper<Object, Text, Text, Text> {

	// Create a counter and initialize with 1
	private final Text error = new Text("1,1");
	private final Text noError = new Text("0,1");
	// Create a hadoop text object to store words
	// private Text word = new Text();

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		try {
			// Generate strings from 1 ride
			String[] ride = value.toString().split(",");
			String medallion = ride[0];
			if (ride.length == 17) {
				// Check if the line is an error based on GPS coordinates (either a 0 or blank)
				// float n = Float.parseFloat(hour);
				try {
					boolean isError = false;
					for (int i = 6; i < 10; i++) {
						if (ride[i].equals("") || Float.parseFloat(ride[i]) == 0) {
							isError = true;
							break;
						}
					}
					if (isError) {
						context.write(new Text(medallion), error);
					} else {
						context.write(new Text(medallion), noError);
					}
					
				} catch (Exception e) {
					System.out.println("Error line");
				}
			} else {
				System.out.println("Not enough attributes!");
			}
		} catch (Exception e) {
			System.out.println("Error line: " + value.toString());
		}
	}
}