package in.dreamlab.iisc.se256;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;



public class SemiClusteringMapper extends Mapper<Object , Text, Text, Text> {

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
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		//edge list input
		String[] data=value.toString().split("\\s+");
		
		if(iteration.equals("0"))
		{
			Cluster c=new Cluster();
			c.AddVertex(data[0], data[1]);
			String[] neighbours=data[1].split(",");
			for(String neighbour:neighbours){
			context.write(new Text(neighbour), new Text(c.toString()));
			}
			
		}else
		{
			String Kbest = BestClusters.get(data[0]);
			
			String[] neighbours=data[1].split(",");
			for(String neighbour:neighbours){
				context.write(new Text(neighbour), new Text(Kbest));
			}
		}
		
		
	}






}


