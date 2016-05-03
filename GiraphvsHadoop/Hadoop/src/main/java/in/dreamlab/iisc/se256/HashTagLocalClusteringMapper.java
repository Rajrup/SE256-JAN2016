package in.dreamlab.iisc.se256;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.mortbay.log.Log;



public class HashTagLocalClusteringMapper extends Mapper<Object , Text, Text, Text> {

	HashMap<String,String> AdjListHashMap;
	
	@Override
	protected void setup(Context context){
		
		//Read adjacency list into a hashmap
		String HashMapPath=context.getConfiguration().get("HashMapPath");
		AdjListHashMap=new HashMap<String,String>();	
		
		try
		{
		//Initializing HashTags of each user
		
	 	FileSystem fs = FileSystem.get(new Configuration());//Change hdfs path accordingly
	 	FileStatus[] status = fs.listStatus(new Path("hdfs://sslcluster:9000/" +HashMapPath));
	 	for (int i=0;i<status.length;i++){
	 		
	 		
	 			BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
	         	String line;
	         	line=br.readLine();
	         	while (line != null){
	        	
	        	     String[] str=line.split("\\s+");
	        	     if(str.length>1)
	        	     {
	        	     AdjListHashMap.put(str[0],str[1]);
	        	     Log.info("KEY *************" +str[0] + " VALUE " + str[1]);
	        	     }
	                 line=br.readLine();
	         	}
			
	 	}
		}catch(Exception e)
		{
			Log.info(e.getMessage());
		}
		
		
	}
	
	
	
	
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
//		Log.info("IN MAP*******");
	
		
		try
		{
		int edgeCount=0;
		
		
		ArrayList<String> LocalSet=new ArrayList<String>();
	
		String[] data=value.toString().split("\\s+");
		LocalSet.add(data[0]);
		if(data.length>1){
			String[] neighbours=data[1].split(",");
		
			for(String neighbour:neighbours){
				LocalSet.add(neighbour);
			}
		
			for(String vertex:LocalSet){
			
				String[] vertexNeighbours=AdjListHashMap.get(vertex).split(",");
				for(String n:vertexNeighbours)
				{
					if(LocalSet.contains(n)){
						edgeCount++;
					}
				}
			
			}
		
		
		}
		
		if(data.length>1){
			int total=LocalSet.size();
			double localClusterCoeff=(double)edgeCount/(total*(total-1));
			context.write(new Text(data[0]), new Text(localClusterCoeff +""));
		}
		else{
			context.write(new Text(data[0]), new Text("0"));
		}
		
		}
		catch(Exception e){
			
			
		}
		
		
	}
}