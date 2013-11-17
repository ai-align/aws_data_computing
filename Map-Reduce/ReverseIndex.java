 
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class ReverseIndex {
	static HashSet<String> stopSet = new   HashSet<String>();                      
	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();
                      
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			
			while (tokenizer.hasMoreTokens()) {
				stopSet.add("a");
				FileSplit fs = (FileSplit) reporter.getInputSplit();
				String location = fs.getPath().getName();
				String s = tokenizer.nextToken().trim();
				Pattern pattern = Pattern.compile("[^a-zA-Z0-9]");
				Matcher matcher = pattern.matcher(s);
				s = matcher.replaceAll("");
				word.set(s);
				if (stopSet.contains(s)){
					//output.collect(new Text("*****"), new Text(location));
					continue;
				}
				output.collect(word, new Text(location));
			}            
		}                
	}                    
                         
	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			HashSet<String> set = new HashSet<String>();
			StringBuilder sb = new StringBuilder();
			long count = 0;
			while (values.hasNext()) {
				// sum += values.next().get();
				count++; 
				Text t = values.next();
				if (!set.contains(t)){
					if (count > 1){
						sb.append(",");
					}    
					set.add(t.toString());
					sb.append(t.toString());
				}        
			}            
			output.collect(key, new Text(sb.toString()));
		}                
	}                    
                         
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(ReverseIndex.class);
		DistributedCache.addCacheFile(new URI("/abc/english.stop"), 
                conf);
		File f = new File("english.stop");
		Scanner scanner = new Scanner(f);
		while (scanner.hasNext()){
			String line = scanner.nextLine().trim();
			stopSet.add(line);
		}
		conf.setJobName("reverseindex");
                         
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