package edu.cmu;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes; 

import java.util.*;

public class model {

    public static class TheMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
      private Text word = new Text();
      private Text info = new Text();
      private int T;
      public void configure(JobConf job) {
          T = Integer.parseInt(job.get("T"));
      }
      public void map(LongWritable key, Text value, OutputCollector<Text, Text> output1, Reporter reporter) throws IOException {
    	
    	String line = value.toString();
    	String text = line.split("\t")[0];
    	int times = Integer.parseInt(line.split("\t")[1]);
        if(times < T) return;
        String[] temp = text.split(" ");
        line = "";
        if(temp.length <= 1) return;
        for(int i=0;i<=temp.length-2;i++){
        	line = line + temp[i];
        	if(i <temp.length-2) line = line+" ";
        }
        if(line.charAt(0)==' ') line = line.substring(1);
        String last = temp[temp.length-1]+" "+times;
        word.set(line);
        info.set(last);
        output1.collect(word, info);
      }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {   
      private int N;
	  private Configuration conf = HBaseConfiguration.create();
	  private HTable table;
      public void configure(JobConf job) {
          N = Integer.parseInt(job.get("N"));
          try {
				 table = new HTable(conf, "model");
				 table.setAutoFlush(false);
				 } catch (IOException e) {
				 e.printStackTrace();
				 }
      }
      public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output2, Reporter reporter) throws IOException {
    	
    	String last = "";
    	int num,sum = 0;
    	Map<String, Integer> list = new HashMap<String, Integer>();
    	while(values.hasNext()){
    		String tempstring = values.next().toString();
        	String[] temp = tempstring.split(" ");
        	last = temp[0];
        	num = Integer.parseInt(temp[1]);
        	list.put(last, num);
        	sum += num;
        }
    	List<Map.Entry<String, Integer>> arrayList = new ArrayList<Map.Entry<String, Integer>>(list.entrySet());
		Collections.sort(arrayList, new Comparator<Map.Entry<String, Integer>>(){
			public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
				return ((Integer) o2.getValue())-((Integer)o1.getValue());
			}
		});
		int total = (N<arrayList.size())?N:arrayList.size();
		Put put = new Put(Bytes.toBytes(key.toString()));
		for(int i=0;i<total;i++){
			String next = arrayList.get(i).getKey();
			double number = arrayList.get(i).getValue();
            double prob = number *  100.0 / sum;
            
    	    put.add(Bytes.toBytes("Probability"),
    	            Bytes.toBytes(next),
    	            Bytes.toBytes("" + prob));
		}
		table.put(put);
      }
      public void close(){
      	try {
				table.flushCommits();
			} catch (IOException e) {
				e.printStackTrace();
			}
      }

    }

    public static void main(String[] args) throws Exception {
      JobConf conf = new JobConf(model.class);
      conf.setJobName("model");

      conf.setOutputKeyClass(Text.class);
      conf.setOutputValueClass(Text.class);

      conf.setMapperClass(TheMap.class);
      //conf.setCombinerClass(Reduce.class);
      conf.setReducerClass(Reduce.class);
      
      conf.setInputFormat(TextInputFormat.class);
      conf.setOutputFormat(TextOutputFormat.class);

      FileInputFormat.setInputPaths(conf, new Path(args[0]));
      FileOutputFormat.setOutputPath(conf, new Path(args[1]));
      conf.set("T",args[2]);
      conf.set("N", args[3]);
      JobClient.runJob(conf);
    }
}

