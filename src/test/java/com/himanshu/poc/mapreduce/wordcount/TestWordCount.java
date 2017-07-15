package com.himanshu.poc.mapreduce.wordcount;

import com.himanshu.poc.mapreduce.wordcount.WordCountMapper;
import com.himanshu.poc.mapreduce.wordcount.WordCountReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by himanshu on 13-07-2017.
 */
public class TestWordCount {
  private Mapper mapper;
  private Reducer reducer;
  private MapDriver mapDriver;
  private ReduceDriver reduceDriver;
  private MapReduceDriver mapReduceDriver;
  private String firstTestKey, secondTestKey;

  @Before
  public void setUp() throws Exception {
    mapper = new WordCountMapper();
    reducer = new WordCountReducer();
    mapDriver = new MapDriver(mapper);
    reduceDriver = new ReduceDriver(reducer);
    mapReduceDriver = new MapReduceDriver(mapper, reducer);
    firstTestKey = "abc";
    secondTestKey = "xyz";
  }

  @Test
  public void testWordCountMapper() throws IOException {
    mapDriver.withInput(new LongWritable(1), new Text(firstTestKey))
        .withInput(new LongWritable(1), new Text(secondTestKey))
        .withOutput(new Text(firstTestKey), new IntWritable(1))
        .withOutput(new Text(secondTestKey), new IntWritable(1))
        .runTest();
  }

  @Test
  public void testWordCountReducer() throws IOException {
    Text firstMapKey = new Text(firstTestKey);
    List<IntWritable> firstMapValues = new ArrayList<>();
    firstMapValues.add(new IntWritable(1));
    firstMapValues.add(new IntWritable(1));

    Text secondMapKey = new Text(secondTestKey);
    List<IntWritable> secondMapValues = new ArrayList<>();
    secondMapValues.add(new IntWritable(1));
    secondMapValues.add(new IntWritable(1));
    secondMapValues.add(new IntWritable(1));

    reduceDriver.withInput(firstMapKey, firstMapValues)
        .withInput(secondMapKey, secondMapValues)
        .withOutput(firstMapKey, new IntWritable(2))
        .withOutput(secondMapKey, new IntWritable(3))
        .runTest();
  }

  @Test
  public void testWordCountMapReducer() throws IOException {
    mapReduceDriver.withInput(new LongWritable(1), new Text(firstTestKey))
        .withInput(new LongWritable(2), new Text(firstTestKey))
        .withInput(new LongWritable(3), new Text(secondTestKey))
        .withOutput(new Text(firstTestKey), new IntWritable(2))
        .withOutput(new Text(secondTestKey), new IntWritable(1))
        .runTest();
  }

}
