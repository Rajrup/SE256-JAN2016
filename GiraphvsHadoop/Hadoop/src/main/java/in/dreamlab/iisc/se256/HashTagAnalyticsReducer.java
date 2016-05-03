package in.dreamlab.iisc.se256;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;




public class HashTagAnalyticsReducer extends Reducer<Text,Text,Text, Text> {
	
	@Override
	protected void reduce(Text key, Iterable<Text> values , Context context)
			throws IOException, InterruptedException {
		String adjList="";
		for(Text v:values){
			adjList=v.toString();
			break;
		}
		
		context.write(key, new Text(adjList));
		
	}

}