package in.dreamlab.iisc.se256;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;



public class SemiClusteringReducer extends Reducer<Text,Text,Text, Text> {
	String iteration;
	HashMap<String,String> BestClusters;
	@Override
	protected void setup(Context context){
		iteration=context.getConfiguration().get("iteration");
		if(!iteration.equals("0")){
		//LOAD HASHMAP of k best clusters
		}
		
	}
	
	
	@Override
	protected void reduce(Text key, Iterable<Text> values , Context context)
			throws IOException, InterruptedException {
		
		
		for(Text v:values){
			
		}
		
			
	}

}