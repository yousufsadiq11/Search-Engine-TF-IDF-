package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TDIDF extends Configured implements Tool 
{

   private static final Logger LOG = Logger .getLogger( TDIDF.class);

   public static void main( String[] args) throws  Exception 
   {
      int res  = ToolRunner .run( new TDIDF(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception 
   {
	  // Creating Job object
      Job job  = Job .getInstance(getConf(), " JOB_1 ");
      job.setJarByClass( this .getClass());
      // Instantiating input and otput paths for Job 1
      FileInputFormat.addInputPaths(job,  args[0]);
      FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));
      // Instantiating it's corresponding Mapper, Reducer, input and output classes
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      job.setOutputKeyClass( Text .class);
      job.setOutputValueClass( IntWritable .class);
      job.waitForCompletion(true);
      	// Creating object for Job 2
      	Job job2 = Job.getInstance(getConf(), "JOB_2");
		job2.setJarByClass(this.getClass());
		// Instantiating it's Mapper, Reducer and Combiner classes
		job2.setMapperClass(Mapper2.class);
		job2.setCombinerClass(Combiner1.class);
		job2.setReducerClass(Reducer2.class);
		// Instantiating input and output paths for Job 2
		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		// Instantiating corresponding output classes
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(TextArrayWritable.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(DoubleWritable.class);

		boolean success = job2.waitForCompletion(true);
		return success ? 0 : 1;
   }
   
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > 
   {
      private final static IntWritable one  = new IntWritable( 1);
      private Text word  = new Text();

      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException 
      {
    	  context.getInputSplit();
    	  int j=0;
    	  // Getting File path and parsing it to obtain the File Name
    	  String filePathString = ((FileSplit) context.getInputSplit()).getPath().toString(); 
    	  for(int i=filePathString.length()-1;i>=0;i--)
    	  {
    		  if(filePathString.charAt(i)=='/')
    			  {j=i+1;break;}
    	  }
    	 System.out.println(filePathString.substring(j, filePathString.length())); 
    	 String line  = lineText.toString();
         Text currentWord  = new Text();
         // Appending word with ##### as Delimiter Followed by File Name
         for ( String word  : WORD_BOUNDARY .split(line)) 
         {
            if (word.isEmpty()) 
            {
               continue;
            }
            currentWord  = new Text(word+"#####"+filePathString.substring(j, filePathString.length()));
            // Writing output of word formed after concatenating it with File Name and setting count as 1
            context.write(currentWord,one);
         }
      }
   }
   
   public static class Reduce extends Reducer<Text ,  IntWritable ,  Text ,  DoubleWritable > 
   {
      @Override 
      public void reduce( Text word,  Iterable<IntWritable > counts,  Context context)
         throws IOException,  InterruptedException 
      {
         System.out.println("reducer");int sum  = 0;double termfreq=0;
         // Determining count of Corresponding Words
         for ( IntWritable count  : counts) 
         {
            sum  += count.get();
         }
         // Computing term frequency
         termfreq=1+Math.log(sum)/Math.log(10);
         context.write(word,  new DoubleWritable(termfreq));
      }
   }
   
   public static class Mapper2 extends
	Mapper<LongWritable, Text, Text, TextArrayWritable> 
   {

	// Creating TextArrayWritable Object
	TextArrayWritable lists = new TextArrayWritable();

public void map(LongWritable offset, Text lineText, Context context)
		throws IOException, InterruptedException 
{
	context.getInputSplit();
	String merge;
	// Seperating input by ##### after converting it to string
	String line = lineText.toString();
	String splitted[] = line.split("#####");
	// Further splitting input by Tab
	String[] further_split = splitted[1].split("\t");
	merge = further_split[0] + "=" + further_split[1];
	System.out.println("c" + " " + splitted[0]);
	// Adding the values to lists object of TextArrayWritable class
	Text[] textValues = new Text[1];
	textValues[0] = new Text(merge);
	lists.set(textValues);
	context.write(new Text(splitted[0]), lists);

 }
}

public static class Reducer2 extends
	Reducer<Text, TextArrayWritable, Text, DoubleWritable> 
{

public void reduce(Text word, Iterable<TextArrayWritable> values,
		Context context) throws IOException, InterruptedException 
{
	System.out.println("Reducer");
	String[] temp = null, further_split = null;
	String t = "";
	System.out.println("two ");
	Double idf = 0d;
	Double tf_idf = 0d;
	// Iterating through List of files and it's corresponding Term frequencies
	for (ArrayWritable a : values) 
	{
		Writable[] Text1 = a.get();
		System.out.println("texxxxxxxt" + Text1[0]);
		t = Text1[0].toString();
		// ;;; indicates presence of more than single file in the list
		if (t.contains(";;;")) 
		{
			// Splitting it by ;;; delimiter and further splitting with = delimiter to get separate term frequency and file name
			temp = t.split(";;;");
			further_split = temp[0].split("=");
			idf = Math.log(2) / Math.log(10);
			tf_idf = idf * Double.parseDouble(further_split[1]);
			// Writing output of two files with its' corresponding TDIDF
			context.write(new Text(word + "#####" + further_split[0]),
					new DoubleWritable(tf_idf));
			further_split = null;
			// Following the same procedure as mentioned for File 1
			further_split = temp[1].split("=");
			idf = Math.log(2) / Math.log(10);
			tf_idf = idf * Double.parseDouble(further_split[1]);
			context.write(new Text(word + "#####" + further_split[0]),
					new DoubleWritable(tf_idf));
			further_split = null;
			System.out.println("temp 1" + temp[1]);
		} 
		// If output list has just single file
		// Computing TDIDF and appending to output
		else 
		{
			further_split = t.split("=");
			idf = Math.log(2) / Math.log(10);
			tf_idf = idf * Double.parseDouble(further_split[1]);
			context.write(new Text(word + "#####" + further_split[0]),
					new DoubleWritable(tf_idf));
			further_split = null;
	  }
	}
  }
}
	// Combiner to append a key value and its corresponding List containing Term frequencies and File Names
	public static class Combiner1 extends
	Reducer<Text, TextArrayWritable, Text, TextArrayWritable> 
	{
		TextArrayWritable lists = new TextArrayWritable();

	public void reduce(Text word, Iterable<TextArrayWritable> values,
		Context context) throws IOException, InterruptedException 
	{
	System.out.println("combiner");
	System.out.println(word);
	String x = "";
	// Combiner which is used to form a list containing filenames and it's corresponding Term Frequencies for a word
	for (ArrayWritable a : values) 
	{
		Writable[] Text1 = a.get();
		if (x.equals(""))
			x = x + Text1[0].toString();
		else
			x = x + ";;;" + Text1[0].toString();
	}
	Text[] textValues = new Text[1];
	textValues[0] = new Text(x);
	lists.set(textValues);
	context.write(word, lists);
  }
}
	// Creating class TextArrayWritable to access ArrayWritable
	public static class TextArrayWritable extends ArrayWritable 
	{
		// Constructor 
		public TextArrayWritable() 
		{
			super(Text.class);
		}
	}
}