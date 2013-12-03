import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.Configuration; 
 
import org.apache.hadoop.hbase.HBaseConfiguration;  
import org.apache.hadoop.hbase.HColumnDescriptor;  
import org.apache.hadoop.hbase.HTableDescriptor;  
import org.apache.hadoop.hbase.client.HBaseAdmin;  
import org.apache.hadoop.hbase.client.HTable;  
import org.apache.hadoop.hbase.client.HTablePool;  
import org.apache.hadoop.hbase.client.Put;

public class textPredict {

    public static class HMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
      
	  //private final static IntWritable one = new IntWritable(1);
      private Text word = new Text();

      public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String oneline = value.toString();
        
        String[] values = oneline.split("\t");
        int count = Integer.parseInt(values[1]);
		String[] words = values[0].split(" ");
		int length = words.length; 
		//filter the count smaller than 2
        if(count > 1 && length > 1){
            String rowKey = "";
        	for(int i = 0; i < length - 1; i++){
        		rowKey = rowKey + " " + words[i];
        	}
        	String nextWord = words[length-1];
            String nextWordCount = nextWord + " " + String.valueOf(count);      
            word.set(rowKey);
            output.collect(word, new Text(nextWordCount));            
        }	
        
      }
    }
    public static class combine extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        	Map<String, Integer> hm = new HashMap<String, Integer>();
        	//int sum = 0;
            while (values.hasNext()) {
              String[] wordCount = values.next().toString().split(" ");
              hm.put(wordCount[0], Integer.parseInt(wordCount[1]));
              //sum += Integer.parseInt(wordCount[1]);
    		  
            }
            List<Map.Entry<String, Integer>> hmList = new ArrayList<Map.Entry<String, Integer>>(hm.entrySet());
            Collections.sort(hmList, new Comparator<Map.Entry<String, Integer>>() {   
                public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {      
                    return (o2.getValue() - o1.getValue()); 
                    //return (o1.getKey()).toString().compareTo(o2.getKey());
                }
            });
    		
            int i = 0; 
            for(Map.Entry<String, Integer> s : hmList){
            	if(i == 5)
    				break;
            	String wordCount = s.getKey() + " " + String.valueOf(s.getValue());
            	output.collect(key, new Text(wordCount));
            	i ++ ;
            }
          }  
	}
    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    	
		
        //Configuration conf = HBaseConfiguration.create();
        //HBaseAdmin admin = new HBaseAdmin(conf);
		
		private Configuration hconf=null;
		public void configure(JobConf conf){
			hconf=HBaseConfiguration.create();
		}
		
		public void insertData(String tableName, String key, String data, String probability) {  
			HTablePool pool = new HTablePool(hconf, 1000);  
			HTable table = (HTable) pool.getTable(tableName);  
			Put put = new Put(key.getBytes());
			put.add("predictFamily".getBytes(), data.getBytes(), probability.getBytes());
			try {  
				table.put(put);  
			} catch (IOException e) {  
				e.printStackTrace();  
			}  
		}  

      public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
    	Map<String, Integer> hm = new HashMap<String, Integer>();
    	//int sum = 0;
        while (values.hasNext()) {
          String[] wordCount = values.next().toString().split(" ");
          hm.put(wordCount[0], Integer.parseInt(wordCount[1]));
          //sum += Integer.parseInt(wordCount[1]);
		  //output.collect(key, new Text(wordCount));
        }
        List<Map.Entry<String, Integer>> hmList = new ArrayList<Map.Entry<String, Integer>>(hm.entrySet());
        Collections.sort(hmList, new Comparator<Map.Entry<String, Integer>>() {   
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {      
                return (o2.getValue() - o1.getValue()); 
                //return (o1.getKey()).toString().compareTo(o2.getKey());
            }
        });
		
        int i = 0; 
        for(Map.Entry<String, Integer> s : hmList){
        	if(i == 5)
				break;
			insertData("predict", key.toString(), s.getKey(), String.valueOf(s.getValue()));
        	i ++ ;
        }
      }  
	  	
    }
 

    public static void main(String[] args) throws Exception {
      JobConf conf = new JobConf(textPredict.class);
      conf.setJobName("textPredict");

      conf.setOutputKeyClass(Text.class);
      conf.setOutputValueClass(Text.class);

      conf.setMapperClass(HMap.class);
      conf.setCombinerClass(Reduce.class);
      conf.setReducerClass(Reduce.class);

      conf.setInputFormat(TextInputFormat.class);
      conf.setOutputFormat(TextOutputFormat.class);

      FileInputFormat.setInputPaths(conf, new Path(args[0]));
      FileOutputFormat.setOutputPath(conf, new Path(args[1]));

      JobClient.runJob(conf);
    }
}



