package com.himanshu.poc.mapreduce.airport;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by himanshu on 15-07-2017.
 */
public class AirportCountryCityMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] airportProps = value.toString().split(",");
    String country = airportProps[3].concat("-").concat(airportProps[2]).replace("\"", "");
    context.write(new Text(country), new IntWritable(1));
  }
}
