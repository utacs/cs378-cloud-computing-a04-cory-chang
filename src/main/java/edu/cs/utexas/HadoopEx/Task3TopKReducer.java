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

public class Task3TopKReducer extends Reducer<Text, Text, Text, Text> {

    private PriorityQueue<Task3WordAndCount> pq = new PriorityQueue<Task3WordAndCount>(10);

    private Logger logger = Logger.getLogger(Task3TopKReducer.class);

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
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        // A local counter just to illustrate the number of values here!
        int counter = 0;

        // size of values is 1 because key only has one distinct value
        for (Text value : values) {

            String[] temp = value.toString().split(",");
            float money = Float.parseFloat(temp[0]);
            int duration = Integer.parseInt(temp[1]);
            logger.info("Reducer Text: counter is " + counter);
            logger.info("Reducer Text: Add this item  "
                    + new Task3WordAndCount(new Text(key), new FloatWritable(money), new IntWritable(duration)).toString());

            pq.add(new Task3WordAndCount(new Text(key), new FloatWritable(money), new IntWritable(duration)));

            logger.info("Reducer Text: " + key.toString() + " , Count: " + value.toString());
            logger.info("PQ Status: " + pq.toString());
        }

        // keep the priorityQueue size <= heapSize
        while (pq.size() > 10) {
            pq.poll();
        }

    }

    public void cleanup(Context context) throws IOException, InterruptedException {
        logger.info("TopKReducer cleanup cleanup.");
        logger.info("pq.size() is " + pq.size());

        List<Task3WordAndCount> values = new ArrayList<Task3WordAndCount>(3);

        while (pq.size() > 0) {
            values.add(pq.poll());
        }

        logger.info("values.size() is " + values.size());
        logger.info(values.toString());

        // reverse so they are ordered in descending order
        Collections.reverse(values);

        for (Task3WordAndCount value : values) {
            context.write(value.getDriver(), new Text("" + ((value.getMoney().get() / (value.getSeconds().get())) * 60)));
            //logger.info("TopKReducer - Top-10 Words are:  " + value.getWord() + "  Count:" + value.getCount());
        }

    }

}