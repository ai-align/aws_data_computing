

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class inverted_Index {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    	
      //private final static LongWritable one = new LongWritable(location);
      private Text word = new Text();

      public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        FileSplit fs = (FileSplit) reporter.getInputSplit();
        String fileName = fs.getPath().getName();
        
    	String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
          word.set(tokenizer.nextToken());
          output.collect(word, new Text(fileName));
        }
      }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
      public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
         String fileSet = new String();    
        while (values.hasNext()) {
          String fileString = values.toString();
          	fileSet = fileSet + fileString;
        }
        output.collect(key, new Text(fileSet));
      }
    }

    public static void main(String[] args) throws Exception {
      JobConf conf = new JobConf(inverted_Index.class);
      conf.setJobName("inverted_Index");

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

