package com.himanshu.poc.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new WordCount(), args);
    System.exit(exitCode);
  }

  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.printf("Usage: %s needs two arguments, input and output files\n", getClass().getSimpleName());
      return -1;
    }

    Configuration conf = getConf();
    conf.set("fs.file.impl", "com.himanshu.poc.mapreduce.fs.WindowsLocalFileSystem");

    //some additional
    conf.set("fs.default.name", "file:///");
    conf.set("mapred.job.tracker", "local");

    Job job = new Job(conf);
    job.setJarByClass(WordCount.class);
    job.setJobName("WordCounter");

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapperClass(WordCountMapper.class);
    job.setReducerClass(WordCountReducer.class);

    int returnValue = job.waitForCompletion(true) ? 0 : 1;

    if (job.isSuccessful()) {
      System.out.println("Job was successful");
    } else if (!job.isSuccessful()) {
      System.out.println("Job was not successful");
    }

    return returnValue;
  }
}