package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.log4j.Logger;

public class Task2TopKMapper extends Mapper<Text, Text, Text, Text> {

	private Logger logger = Logger.getLogger(Task2TopKMapper.class);

	private PriorityQueue<Task2WordAndCount> pq;

	public void setup(Context context) {
		pq = new PriorityQueue<>();

	}

	/**
	 * Reads in results from the first job and filters the topk results
	 *
	 * @param key
	 * @param value a float value stored as a string
	 */
	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] temp = value.toString().split(",");
		int errors = Integer.parseInt(temp[0]);
		int total = Integer.parseInt(temp[1]);

		pq.add(new Task2WordAndCount(new Text(key), new IntWritable(errors), new IntWritable(total)));

		if (pq.size() > 5) {
			pq.poll();
		}
	}

	public void cleanup(Context context) throws IOException, InterruptedException {

		while (pq.size() > 0) {
			Task2WordAndCount wordAndCount = pq.poll();
			context.write(wordAndCount.getWord(), new Text("" + ((float)wordAndCount.getErrors().get() / wordAndCount.getTotal().get())));
			logger.info("TopKMapper PQ Status: " + pq.toString());
		}
	}

}