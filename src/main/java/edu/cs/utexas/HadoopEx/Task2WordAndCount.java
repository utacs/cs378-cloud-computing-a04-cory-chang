package edu.cs.utexas.HadoopEx;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class Task2WordAndCount implements Comparable<Task2WordAndCount> {

        private final Text word;
        private final IntWritable errors;
        private final IntWritable total;

        public Task2WordAndCount(Text word, IntWritable errors, IntWritable total) {
            this.word = word;
            this.errors = errors;
            this.total = total;
        }

        public Text getWord() {
            return word;
        }

        public IntWritable getErrors() {
            return errors;
        }

        public IntWritable getTotal() {
            return total;
        }



    /**
     * Compares two sort data objects by their value.
     * @param other
     * @return 0 if equal, negative if this < other, positive if this > other
     */
        @Override
        public int compareTo(Task2WordAndCount other) {
            float diff = ((float) this.errors.get() / this.total.get()) - ((float) other.errors.get() / other.total.get());
            if (diff > 0) {
                return 1;
            } else if (diff < 0) {
                return -1;
            }
            return 0;
        }

        public String toString(){
            float percentage = ((float) this.errors.get() / this.total.get());
            return "("+word.toString() +" , "+ String.valueOf(percentage*100)+")";
        }
    }