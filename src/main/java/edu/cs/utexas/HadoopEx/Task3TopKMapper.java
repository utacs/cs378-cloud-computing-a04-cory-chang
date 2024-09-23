package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.PriorityQueue;


import org.apache.log4j.Logger;


public class Task3TopKMapper extends Mapper<Text, Text, Text, Text> {

	private Logger logger = Logger.getLogger(Task3TopKMapper.class);


	private PriorityQueue<Task3WordAndCount> pq;

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
		
		float money = Float.parseFloat(temp[0]);
        int duration = Integer.parseInt(temp[1]);

		pq.add(new Task3WordAndCount(new Text(key), new FloatWritable(money), new IntWritable(duration)) );

		if (pq.size() > 10) {
			pq.poll();
		}
	}

	public void cleanup(Context context) throws IOException, InterruptedException {

		System.out.println("cleaning up");
		while (pq.size() > 0) {
			Task3WordAndCount wordAndCount = pq.poll();
			context.write(wordAndCount.getDriver(), new Text(wordAndCount.getMoney().get() + "," + wordAndCount.getSeconds().get()));
			logger.info("TopKMapper PQ Status: " + pq.toString());
		}
	}

}