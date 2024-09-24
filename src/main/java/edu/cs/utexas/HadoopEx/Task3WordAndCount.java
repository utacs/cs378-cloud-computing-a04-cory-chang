package edu.cs.utexas.HadoopEx;


import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class Task3WordAndCount implements Comparable<Task3WordAndCount> {

        private final Text driver;
        private final FloatWritable money;
        private final IntWritable seconds;

        public Task3WordAndCount(Text driver, FloatWritable money, IntWritable seconds) {
            this.driver = driver;
            this.money = money;
            this.seconds = seconds;
        }

        public Text getDriver() {
            return driver;
        }

        public FloatWritable getMoney() {
            return money;
        }

        public IntWritable getSeconds() {
            return seconds;
        }
    /**
     * Compares two sort data objects by their value.
     * @param other
     * @return 0 if equal, negative if this < other, positive if this > other
     */
        @Override
        public int compareTo(Task3WordAndCount other) {

            float diff = (this.money.get() / this.seconds.get()) - (other.money.get() / other.seconds.get());
            if (diff > 0) {
                return 1;
            } else if (diff < 0) {
                return -1;
            }
            return 0;
        }

        public String toString(){
            float diff = ((this.money.get() / this.seconds.get()) * 60);
            return "("+this.driver.toString() +" , "+ diff +")";
        }
    }

