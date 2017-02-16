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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class TermFrequency extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( TermFrequency.class);

   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new TermFrequency(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
	  // Creating an Object for Job
      Job job  = Job .getInstance(getConf(), " TermFrequency ");
      job.setJarByClass( this .getClass());
      // Instantiating input and output File paths
      FileInputFormat.addInputPaths(job,  args[0]);
      FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));
      // Setting corresponding Mapper and Reducer classes
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      // Setting Type of Output Key and Value classes
      job.setOutputKeyClass( Text .class);
      job.setOutputValueClass( IntWritable .class);

      return job.waitForCompletion( true)  ? 0 : 1;
   }
   
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
      private final static IntWritable one  = new IntWritable( 1);
      private Text word  = new Text();
      // Instantiating Pattern
      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {
    	  context.getInputSplit();
    	  int j=0;
    	  // Getting File path and parsing it to obtain the File Name
    	  String filePathString = ((FileSplit) context.getInputSplit()).getPath().toString(); 
    	  for(int i=filePathString.length()-1;i>=0;i--){
    		  if(filePathString.charAt(i)=='/')
    			  {j=i+1;break;}
    	  }
    	 System.out.println(filePathString.substring(j, filePathString.length())); 
    	  String line  = lineText.toString().toLowerCase();
         Text currentWord  = new Text();
         // Appending word with ##### as Delimiter Followed by File Name
         for ( String word  : WORD_BOUNDARY .split(line)) {
            if (word.isEmpty()) {
               continue;
            }
            currentWord  = new Text(word+"#####"+filePathString.substring(j, filePathString.length()));
            // Writing output of word formed after concatenating it with File Name and setting count as 1
            context.write(currentWord,one);
         }
      }
   }

   public static class Reduce extends Reducer<Text ,  IntWritable ,  Text ,  DoubleWritable > {
      @Override 
      public void reduce( Text word,  Iterable<IntWritable > counts,  Context context)
         throws IOException,  InterruptedException {
         int sum  = 0;double termfreq=0;
         // Determining count of Corresponding Words
         for ( IntWritable count  : counts) {
            sum  += count.get();
         }
         // Computing term frequency
         termfreq=1+Math.log(sum)/Math.log(10);
         context.write(word,  new DoubleWritable(termfreq));
      }
   }
}