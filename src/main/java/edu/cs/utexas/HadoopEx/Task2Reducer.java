package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task2Reducer extends  Reducer<Text, Text, Text, Text> {

   public void reduce(Text text, Iterable<Text> values, Context context)
           throws IOException, InterruptedException {
	   
       int errors = 0;
       int total = 0;
       
       for (Text value : values) {
            String[] temp = value.toString().split(",");
            errors += Float.parseFloat(temp[0]);
            total++;
       }
       
       context.write(text, new Text(errors + "," + total));
   }
}