package in.dreamlab.iisc.se256;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.commons.collections.IteratorUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;



public class TriangleCountingReducer extends Reducer<Text,Text,Text, Text> {

	
	 
	@Override
	protected void reduce(Text key, Iterable<Text> values , Context context)
			throws IOException, InterruptedException {
//		System.out.println("***************IN REDUCE");

	String neighbours="";
		
		for(Text v: values){
			if(!neighbours.equals(""))
			neighbours+="," + v.toString();
			else
				neighbours=v.toString();
				
		}
	String[] neighbour_list=neighbours.split(",");

	
	for(int i=0;i<neighbour_list.length && neighbour_list.length>=2;i++){
		for(int j=i+1;j<neighbour_list.length;j++){
			
			if(Long.valueOf(neighbour_list[i]) < Long.valueOf(neighbour_list[j]))
			context.write(key,new Text(neighbour_list[i] +"," +  neighbour_list[j]));
			else
				context.write(key,new Text(neighbour_list[j] +"," +  neighbour_list[i]));
		}
	}

	}

}