package org.myorg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.*;

public class Search extends Configured implements Tool 
{

	private static final Logger LOG = Logger.getLogger(Search.class);

	public static void main(String[] args) throws Exception 
	{
		int res = ToolRunner.run(new Search(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception 
	{
		// Creating Configuration object to pass input search sequence
		Configuration conf = new Configuration();
		ArrayList<String> argsList = new ArrayList<String>();
		for (int i = 2; i < args.length; i++)
			argsList.add(args[i]);
		String[] arr = argsList.toArray(new String[argsList.size()]);
		conf.setStrings("argsList", arr);
		// Creating Job Object
		Job job3 = Job.getInstance(conf, "JOB_3");
		job3.setJarByClass(this.getClass());
		// Instantiating Mapper and Reducer Classes
		job3.setMapperClass(SearchMapper.class);
		job3.setReducerClass(SearchReducer.class);
		// Instantiating File input and output paths
		FileInputFormat.addInputPath(job3, new Path(args[0]));
		FileOutputFormat.setOutputPath(job3, new Path(args[1]));
		// Instantiating corresponding Output classes
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(DoubleWritable.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(DoubleWritable.class);

		boolean success = job3.waitForCompletion(true);
		return success ? 0 : 1;

	}

	public static class SearchMapper extends
			Mapper<LongWritable, Text, Text, DoubleWritable> 
	{

		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException 
		{
			context.getInputSplit();
			// Converting Text to string
			String line = lineText.toString();
			// Seperating it by ##### Delimiter followed by separation with '='
			String[] seperate_line = line.split("#####");
			String[] further_split = seperate_line[1].split("\t");
			// Getting Data from configuration object and comparing it with words in TDIDF File
			Configuration conf = context.getConfiguration();
			ArrayList<String> argsList = new ArrayList<String>(
					Arrays.asList(conf.getStrings("argsList")));
			double pass = Double.parseDouble(further_split[1]);
			for (int i = 0; i < argsList.size(); i++) 
			{
				// Comparing words in a file with elements in search Query
				if (seperate_line[0].equals(argsList.get(i))) 
				{
					// If Element matches then writing the words and it's corresponding TDIDF to output
					context.write(new Text(further_split[0]),
							new DoubleWritable(pass));
				}
			}
		}
	}

	public static class SearchReducer extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> 
	{

		public void reduce(Text word, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException 
		{
			double sum = 0;
			// Computing Sum of corresponding words and Writing to output
			for (DoubleWritable count : values) 
			{
				sum += count.get();
			}
			context.write(word, new DoubleWritable(sum));
		}
	}
}