package org.myorg;

import java.io.IOException;
import java.util.ArrayList;
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


public class TDIDF extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( TDIDF.class);

   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new TDIDF(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
      Job job  = Job .getInstance(getConf(), " JOB_1 ");
      job.setJarByClass( this .getClass());

      FileInputFormat.addInputPaths(job,  args[0]);
      FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));
  
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      job.setOutputKeyClass( Text .class);
      job.setOutputValueClass( IntWritable .class);
     job.waitForCompletion(true);
          Job job2 = Job.getInstance(getConf(), "JOB_2");
          job2.setJarByClass( this .getClass());
          job2.setMapperClass(Mapper2.class);
          job2.setReducerClass(Reducer2.class);
        
          FileInputFormat.addInputPath(job2, new Path(args[1]));
          FileOutputFormat.setOutputPath(job2, new Path(args[2]));
          job2.setOutputKeyClass( Text .class);
          job2.setOutputValueClass( IntWritable .class);

          boolean success = job2.waitForCompletion(true);
      
      
      return success?0:1;
      
      
   }
   
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
      private final static IntWritable one  = new IntWritable( 1);
      private Text word  = new Text();

      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {
    	  context.getInputSplit();
    	  int j=0;
    	  // Parsing the input file path
    	  String filePathString = ((FileSplit) context.getInputSplit()).getPath().toString(); 
    	  for(int i=filePathString.length()-1;i>=0;i--){
    		  if(filePathString.charAt(i)=='/')
    			  {j=i+1;break;}
    	  }
    	 System.out.println(filePathString.substring(j, filePathString.length())); 
    	  String line  = lineText.toString();
         Text currentWord  = new Text();

         for ( String word  : WORD_BOUNDARY .split(line)) {
            if (word.isEmpty()) {
               continue;
            }
            currentWord  = new Text(word+"#####"+filePathString.substring(j, filePathString.length()));
            context.write(currentWord,one);
         }
      }
   }

   public static class Mapper2 extends Mapper<LongWritable ,  Text ,  Text ,  ArrayWritable > {
	      private final static IntWritable one  = new IntWritable( 100);
	      ArrayWritable lists = new ArrayWritable(Text.class) ;
	      private Text word  = new Text();

	      private static final Pattern WORD_BOUNDARY = Pattern .compile("(.*|\n)*");

	      public void map( LongWritable offset,  Text lineText,  Context context)
	        throws  IOException,  InterruptedException {
	    	  context.getInputSplit();
	    	  int j=0;
	    	  // Parsing the input file path
	    	  String filePathString = ((FileSplit) context.getInputSplit()).getPath().toString(); 
	    	  for(int i=filePathString.length()-1;i>=0;i--){
	    		  if(filePathString.charAt(i)=='/')
	    			  {j=i+1;break;}
	    	  }
	    	 
	    	  String line  = lineText.toString();
	         Text currentWord  = new Text();

	         for ( String word  : WORD_BOUNDARY .split(line)) {
	            if (word.isEmpty()) {
	               continue;
	            }
	            currentWord  = new Text(word+"#####"+filePathString.substring(j, filePathString.length()));
	            context.write(currentWord,lists);
	         }
	      }
	   }
   
   public static class Reducer2 extends Reducer<Text ,  ArrayWritable ,  Text ,  DoubleWritable > {
	      
	      public void reduce( Text word,  Iterable<IntWritable > counts,  Context context)
	         throws IOException,  InterruptedException {
	         int sum  = 0;double termfreq=0;
	         for ( IntWritable count  : counts) {
	            sum  += count.get();
	         }
	         termfreq=1+Math.log(sum)/Math.log(10);
	         context.write(word,  new DoubleWritable(termfreq));
	      }
	   }
   
   
   
   
   
   public static class Reduce extends Reducer<Text ,  IntWritable ,  Text ,  DoubleWritable > {
      @Override 
      public void reduce( Text word,  Iterable<IntWritable > counts,  Context context)
         throws IOException,  InterruptedException {
         System.out.println("reducer");int sum  = 0;double termfreq=0;
         for ( IntWritable count  : counts) {
            sum  += count.get();
         }
         termfreq=1+Math.log(sum)/Math.log(10);
         context.write(word,  new DoubleWritable(termfreq));
      }
      
   }
   
   public static class TextArrayWritable extends ArrayWritable
   {
      public TextArrayWritable()
     {
       super(Text.class) ;
     }
   }
}