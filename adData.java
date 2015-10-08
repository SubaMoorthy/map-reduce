import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;


public class adData {
	/* Map*/
	 public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		 public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			 String line = value.toString();
			 line = line.replace("{", "");
			 line = line.replace("}", "");
			 line = line.replace("\"", "");
			 StringTokenizer tokens = new StringTokenizer(line, ",");
			 Boolean eventFlag = false;
			 String pv = "", event = "";
			
			 while (tokens.hasMoreTokens()) {
				 String currToken = tokens.nextToken();
				 String[] keyValue = currToken.split(":");
				 
				 if (keyValue[0].equals( "e")) {
					 eventFlag = true;
					 if (keyValue[1].equalsIgnoreCase("view") || keyValue[1].equalsIgnoreCase("click")) {
						event = keyValue[1];
					}
				 }
				 if (keyValue[0].equalsIgnoreCase( "pv")) {
					 pv = keyValue[1];
				 }
				 
			 }
			 
			 if (eventFlag && event != "") {
				 output.collect(new Text(pv), new Text(event));
			 }
			 if (!eventFlag) {
				 output.collect(new Text(pv), new Text("asset"));
			 }
		 }
	 }

	 /* Reducer*/
	 public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text,Text> {
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			int clickCnt = 0, viewCnt = 0, assetCnt = 0, totalCnt = 0;
			while (values.hasNext()) {
				String currValue = values.next().toString();
				if (currValue.equalsIgnoreCase("click")) {
					clickCnt+=1;
				}
				if (currValue.equalsIgnoreCase("view")) {
					viewCnt+=1;
				}
				if (currValue.equalsIgnoreCase("asset")) {
					assetCnt+=1;
				}
		
				
			}
			if (assetCnt > 0) {
				String counts = Integer.toString(assetCnt) + " " + Integer.toString(viewCnt) + " " + Integer.toString(clickCnt);
				output.collect(key, new Text(counts));	
			}
		}
	 }
	 
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(adData.class);
		conf.setJobName("adData");
		conf.setJarByClass(adData.class);
		
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		
		JobClient.runJob(conf);

	}
}

