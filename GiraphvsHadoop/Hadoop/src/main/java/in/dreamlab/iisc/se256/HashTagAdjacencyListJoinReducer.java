package in.dreamlab.iisc.se256;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class HashTagAdjacencyListJoinReducer extends Reducer<Text,Text,Text, Text> {
	
	@Override
	protected void reduce(Text key, Iterable<Text> values , Context context)
			throws IOException, InterruptedException {

		String adjList="";
		String HashTags="";
		for(Text v:values){
			if(v.toString().contains(":")){
				
				HashTags=v.toString();
				//removing timestep data from the Hash Tags
				String temp="";
				for(String tag:HashTags.split("\\$"))
				{
					if(temp.equals("")){
						temp=tag.split(":")[0];
					}
					else{
						temp+="$" + tag.split(":")[0];
					}
				}
				
				HashTags=String.valueOf(temp);
			}
			else{
				
				adjList=v.toString();
				
			}
		}
		
		context.write(key, new Text(HashTags + " " + adjList));//joined output
	
	
	}

}