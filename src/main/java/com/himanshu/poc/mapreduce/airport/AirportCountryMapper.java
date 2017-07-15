package com.himanshu.poc.mapreduce.airport;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by himanshu on 15-07-2017.
 */
public class AirportCountryMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] prop = value.toString().split("\t");
    String[] airportProps = prop[0].toString().split("-");
    String country = airportProps[0];
    context.write(new Text(country), new IntWritable(Integer.parseInt(prop[1])));
  }
}
