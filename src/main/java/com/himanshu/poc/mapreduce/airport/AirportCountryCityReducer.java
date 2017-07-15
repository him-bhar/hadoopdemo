package com.himanshu.poc.mapreduce.airport;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by himanshu on 15-07-2017.
 */
public class AirportCountryCityReducer extends Reducer<Text, IntWritable, Text, LongWritable> {
  @Override
  protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    Iterator<IntWritable> value = values.iterator();
    long sum = 0;
    while (value.hasNext()) {
      sum += value.next().get();
    }
    context.write(key, new LongWritable(sum));
  }
}
