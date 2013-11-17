

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.httpclient.URI;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.*;
import org.apache.oro.util.Cache;
import org.eclipse.core.internal.filesystem.local.LocalFile;

public class inverted_Index_bonus {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    	
      
	  public HashSet<String> stopRecord = new HashSet<String>();
	  public Path[] localFilePath;
  	  
  	  public void configure(JobConf job){
  		  try{
  			  localFilePath = DistributedCache.getLocalCacheFiles(job);
  			  String stopWord;
  			  File cacheFile = new File(localFilePath.toString());
  			  Scanner scanner = new Scanner(cacheFile);
  			  while(scanner.hasNextLine()){
  				  stopWord = scanner.nextLine().trim();
  				  stopRecord.add(stopWord);
  			  }
  		  }catch(IOException e){
  			  e.printStackTrace();
  		  }
  	  }
	  
      private Text word = new Text();

      public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        FileSplit fs = (FileSplit) reporter.getInputSplit();
        String fileName = fs.getPath().getName();
        
    	String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
          String temp = new String();
          temp = tokenizer.nextToken();
          temp = temp.toLowerCase().replaceAll("[^a-z]", "");
          if(temp.isEmpty()||stopRecord.contains(temp)){
        	  continue;
          }
          //Pattern pattern = Pattern.compile("[^A-Z|a-z|0-9]");	
          //Matcher matcher = pattern.matcher(temp);
          word.set(new Text(temp));
          output.collect(word, new Text(fileName));
        }
      }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        
	  public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
         
		String fileSet = new String();
        HashSet<String> record = new HashSet<String>();
        while (values.hasNext()) {
       
            String fileString = values.next().toString();
            if(record.contains(fileString)||fileString.isEmpty())
            	continue;
            else{
            	if(fileSet.isEmpty())
            		fileSet = fileString;
            	else
            		fileSet = fileSet + "," + fileString;
            	
            	record.add(fileString);
            }     	
        }
        output.collect(key, new Text(fileSet));
      }
    }

    public static void main(String[] args) throws Exception {
      JobConf conf = new JobConf(inverted_Index_bonus.class);
      conf.setJobName("inverted_Index");
	  
	  Path path = new Path("/abc/englishstop.txt");
	  DistributedCache.addCacheFile(path.toUri(), conf);

      conf.setOutputKeyClass(Text.class);
      conf.setOutputValueClass(Text.class);

      conf.setMapperClass(Map.class);
      conf.setCombinerClass(Reduce.class);
      conf.setReducerClass(Reduce.class);

      conf.setInputFormat(TextInputFormat.class);
      conf.setOutputFormat(TextOutputFormat.class);

      FileInputFormat.setInputPaths(conf, new Path(args[0]));
      FileOutputFormat.setOutputPath(conf, new Path(args[1]));

      JobClient.runJob(conf);
    }
}

