package org.myorg;

import org.apache.hadoop.io.* ;



public class WritablesTest
{
    public static class TextArrayWritable extends ArrayWritable {  
        public TextArrayWritable() {  
        super(Text.class);  
        }  
         
        public TextArrayWritable(String[] strings) {  
        super(Text.class);  
        Text[] texts = new Text[strings.length];  
        for (int i = 0; i < strings.length; i++) {  
        texts[i] = new Text(strings[i]);  
        }  
        set(texts);  
        }  
       }  

  

  public static void main(String[] args)
  {


 
   ArrayWritable b = new ArrayWritable(Text.class) ;
   b.set( new Text[]{ new Text("Hello"), new Text("Writables"), new Text("World !!!")}) ;
   StringBuilder sb = new StringBuilder();

   for (Text value : (Text[])b.get()) {
	
       sb.append(new Text(value) + " ");
   }

 System.out.println(sb);

  
   for (Text i: (Text[])b.get())
   System.out.println(i) ;

    }
}