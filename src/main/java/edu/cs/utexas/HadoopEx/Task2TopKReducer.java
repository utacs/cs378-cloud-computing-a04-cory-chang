package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.PriorityQueue;
import java.util.Iterator;

public class Task2TopKReducer extends Reducer<Text, Text, Text, Text> {

    private PriorityQueue<Task2WordAndCount> pq = new PriorityQueue<Task2WordAndCount>(5);

    private Logger logger = Logger.getLogger(Task2TopKReducer.class);

    // public void setup(Context context) {
    //
    // pq = new PriorityQueue<WordAndCount>(10);
    // }

    /**
     * Takes in the topK from each mapper and calculates the overall topK
     * 
     * @param text
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        // A local counter just to illustrate the number of values here!
        int counter = 0;

        // size of values is 1 because key only has one distinct value
        for (IntWritable value : values) {
            String[] temp = value.toString().split(",");
            int errors = Integer.parseInt(temp[0]);
            int total = Integer.parseInt(temp[1]);
            logger.info("Reducer Text: counter is " + counter);
            logger.info("Reducer Text: Add this item  "
                    + new Task2WordAndCount(new Text(key), new IntWritable(errors), new IntWritable(total)).toString());

            pq.add(new Task2WordAndCount(new Text(key), new IntWritable(errors), new IntWritable(total)));

            logger.info("Reducer Text: " + key.toString() + " , Count: " + value.toString());
            logger.info("PQ Status: " + pq.toString());
        }

        // keep the priorityQueue size <= heapSize
        while (pq.size() > 5) {
            pq.poll();
        }

    }

    public void cleanup(Context context) throws IOException, InterruptedException {
        logger.info("TopKReducer cleanup cleanup.");
        logger.info("pq.size() is " + pq.size());

        List<Task2WordAndCount> values = new ArrayList<Task2WordAndCount>(5);

        while (pq.size() > 0) {
            values.add(pq.poll());
        }

        logger.info("values.size() is " + values.size());
        logger.info(values.toString());

        // reverse so they are ordered in descending order
        Collections.reverse(values);

        for (Task2WordAndCount value : values) {
            context.write(value.getWord(), new Text("hello" + ((float)value.getErrors().get() / value.getTotal().get())));
        }

    }

}