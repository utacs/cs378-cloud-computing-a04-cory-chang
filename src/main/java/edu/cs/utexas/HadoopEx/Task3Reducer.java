package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task3Reducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text text, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        float money = 0;
        int duration = 0;
        
        // Reducing them on the basis of money and duration
        for (Text value : values) {
            String[] temp = value.toString().split(",");
            money += Float.parseFloat(temp[0]);
            duration += Integer.parseInt(temp[1]);
            
        }

        context.write(text, new Text(money + "," + duration));
    }
}