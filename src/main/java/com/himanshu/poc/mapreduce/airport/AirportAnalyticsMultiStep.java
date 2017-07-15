package com.himanshu.poc.mapreduce.airport;

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

/**
 * Created by himanshu on 15-07-2017.
 */
public class AirportAnalyticsMultiStep extends Configured implements Tool {
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new AirportAnalyticsMultiStep(), args);
    System.exit(exitCode);
  }

  public int run(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.printf("Usage: %s needs three arguments, input and intermediate output and final output files\n", getClass().getSimpleName());
      return -1;
    }

    Configuration conf = getConf();
    conf.set("fs.file.impl", "com.himanshu.poc.mapreduce.fs.WindowsLocalFileSystem");

    //some additional
    //conf.set("fs.default.name", "file:///");
    conf.set("mapred.job.tracker", "local");

    Job job = new Job(conf);
    job.setJarByClass(AirportAnalyticsMultiStep.class);
    job.setJobName("AirportAnalytics_CountryCity");

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapperClass(AirportCountryCityMapper.class);
    job.setReducerClass(AirportCountryCityReducer.class);

    int returnValue = job.waitForCompletion(true) ? 0 : 1;

    if (job.isSuccessful()) {
      System.out.println("Job was successful");
    } else if (!job.isSuccessful()) {
      System.out.println("Job was not successful");
    }

    Job job2 = new Job(conf);
    job2.setJarByClass(AirportAnalyticsMultiStep.class);
    job2.setJobName("AirportAnalytics_Country");

    FileInputFormat.addInputPath(job2, new Path(args[1]));
    FileOutputFormat.setOutputPath(job2, new Path(args[2]));

    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class);
    job2.setOutputFormatClass(TextOutputFormat.class);

    job2.setMapperClass(AirportCountryMapper.class);
    job2.setReducerClass(AirportCountryReducer.class);

    int returnValue2 = job2.waitForCompletion(true) ? 0 : 1;

    if (job2.isSuccessful()) {
      System.out.println("Job was successful");
    } else if (!job.isSuccessful()) {
      System.out.println("Job was not successful");
    }

    return returnValue2;
  }
}
