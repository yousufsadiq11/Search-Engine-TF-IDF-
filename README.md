# Search_Engine_Based_On_TF-IDF_Values

Compiler and Platform

---------------------------------------------------------------------------
-Compiler:     	Java compiler

-Java version: 	JDK 1.7.0

-Programming:  Java

-Platform:     	Eclipse IDE

Compilation and execution in Terminal

Step to create folder in HDFS: Commands to create input folder in hdfs

1.	sudo su hdfs

2.	Hadoop fs -mkdir /user/cloudera

3.	Hadoop fs -chown cloudera /user/cloudera

4.	Exit

5.	Sudo su cloudera

6.	Hadoop fs -mkdir /user/cloudera/input

After Creating folder in HDFS add files to HDFS

Step 1: Place the input files in HDFS.

Command: hadoop fs -put < CanterburyFile folder Path> <Path in HDFS>

Example: Hadoop fs -put /home/cloudera/Downloads/cantrbry/Canterbury/ /user/cloudera/input




DOCWORDCOUNT

DocWordCount.java: It attaches file Name to it's corresponding word separated by ##### Delimiter. 
The count of the word in the corresponding file is separated by Tab(\t). 
This program is dynamic and can be executed for any number of input files.

Step 1: Compile java file

Command: 

1.	mkdir -p build

2.	Java -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* <JAVA FILE PATH> -d build -Xlint

Example: Java -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* /home/cloudera/workspace/training/src/org/myorg/DocWordCount.java -d build -Xlint

Step 2: JAR Generation

Command:  jar -cvf <JAR PATH> -C build/ .

Example: jar -cvf DocWordCount.jar -C build/ .

Step 3: Running JAR

Command: Hadoop jar <JAR PATH> <Package Name of JAVA CLASS> <Input Folder Path in HDFS> <Output Folder Path in HDFS>

Example: Hadoop jar DocWordCount.jar org.myorg.DocWordCount /user/cloudera/input /user/cloudera/DocWordCount_Output

To Run the program again Delete the output Folder or change the Output folder Name during Execution.

Command to delete output folder: Hadoop fs -rm -r <Output Folder Path>

Example: Hadoop fs -rm -r /home/cloudera/DocWordCount_Output








TERMFREQUENCY

TermFrequency.java: It computes the Term frequency of each corresponding word. 

Executing Commands: <Input File Folder Path> <Output Folder Name Path>

Example: /home/cloudera/Downloads/cantrbry/ /home/cloudera/Desktop/TermFrequency

Here /home/cloudera/Downloads/cantrbry/ is Input File Path containing both Alice29.txt and asyoulik.txt files.

Step 1: Compile java file

Command: 

1.	mkdir -p build

2.	Java -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* <JAVA FILE PATH> -d build -Xlint

Example: Java -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* /home/cloudera/workspace/training/src/org/myorg/TermFrequency.java -d build -Xlint

Step 2: JAR Generation

Command:  jar -cvf <JAR PATH> -C build/ .

Example: jar -cvf TermFrequency.jar -C build/ .

Step 3: Running JAR

Command: Hadoop jar <JAR PATH> <Package Name of JAVA CLASS> <Input Folder Path in HDFS> <Output Folder Path in HDFS>

Example: Hadoop jar TermFrequency.jar org.myorg.TermFrequency/user/cloudera/input /user/cloudera/TermFrequency_Output

To Run the program again Delete the output Folder or change the Output folder Name during Execution.

Command to delete output folder: Hadoop fs -rm -r <Output Folder Path>

Example: Hadoop fs -rm -r /home/cloudera/ TermFrequency_Output






TFIDF

TFIFD.java: It consists of two jobs. The first job computes the term frequency. 
The output of First Job (i.e Term Frequency) is an input to second job. 
The second job computes TDIFD for each word based on the number of files. 

Step 1: Compile java file

Command: 

1.	mkdir -p build

2.	Java -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* <JAVA FILE PATH> -d build -Xlint

Example: Java -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* /home/cloudera/workspace/training/src/org/myorg/TFIDF.java -d build -Xlint

Step 2: JAR Generation

Command:  jar -cvf <JAR PATH> -C build/ .

Example: jar -cvf TFIDF.jar -C build/ .

Step 3: Running JAR

Command: Hadoop jar <JAR PATH> <Package Name of JAVA CLASS> <Input Folder Path in HDFS> <Intermediate Folder Path> <Output Folder Path in HDFS>

Example: Hadoop jar TFIDF.jar org.myorg.TFIDF/ user/cloudera/input user/cloudera/Intermediate_Output /user/cloudera/TFIDF_Output

To Run the program again Delete the output Folder or change the Output folder Name during Execution.
Command to delete output folder: Hadoop fs -rm -r <Output Folder Path>

Example: 

Hadoop fs -rm -r /home/cloudera/ TFIDF_Output

Hadoop fs -rm -r /home/cloudera/ Intermediate_Output






SEARCH

Search.java: It generates the list of files containing the matching query. 

Step 1: Compile java file

Command: 

1.	mkdir -p build

2.	Java -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* <JAVA FILE PATH> -d build -Xlint

Example: Java -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* /home/cloudera/workspace/training/src/org/myorg/Search.java -d build -Xlint

Step 2: JAR Generation

Command:  jar -cvf <JAR PATH> -C build/ .

Example: jar -cvf Search.jar -C build/ .

Step 3: Running JAR

Command: Hadoop jar <JAR PATH> <Package Name of JAVA CLASS> <Input Folder Path in HDFS> <Output Folder Path in HDFS> <QUERY>

Example 1: Hadoop jar Search.jar org.myorg.Search/ user/cloudera/input /user/cloudera/Search_Output computer science

To Run the program again Delete the output Folder or change the Output folder Name during Execution.

Command to delete output folder: Hadoop fs -rm -r <Output Folder Path>

Example: Hadoop fs -rm -r /home/cloudera/ Search_Output







RANK

Rank.java: It generates the list of files sorted according to the values. 

Step 1: Compile java file

Command: 

mkdir -p build

Java -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* <JAVA FILE PATH> -d build -Xlint

Example: Java -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* /home/cloudera/workspace/training/src/org/myorg/Rank.java -d build -Xlint

Step 2: JAR Generation

Command:  jar -cvf <JAR PATH> -C build/ .

Example: jar -cvf Rank.jar -C build/ .

Step 3: Running JAR

Command: Hadoop jar <JAR PATH> <Package Name of JAVA CLASS> <Input Folder Path in HDFS> <Output Folder Path in HDFS> <Query>

Example 1: Hadoop jar Search.jar org.myorg.Rank/ user/cloudera/input /user/cloudera/Rank_Output computer science
To Run the program again Delete the output Folder or change the Output folder Name during Execution.

Command to delete output folder: Hadoop fs -rm -r <Output Folder Path>

Example: 

Hadoop fs -rm -r /home/cloudera/ Rank_Output

