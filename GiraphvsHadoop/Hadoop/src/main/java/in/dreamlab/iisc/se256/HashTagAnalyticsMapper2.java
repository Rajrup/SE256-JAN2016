package in.dreamlab.iisc.se256;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.mortbay.log.Log;


public class HashTagAnalyticsMapper2 extends Mapper<Object , Text, Text, Text> {


	
	
	HashMap<String,String> VerticesHashMap;
	
	@Override
	protected void setup(Context context){
	//Read the output of previous output in HashMap	
	String HashMapPath=context.getConfiguration().get("HashMapPath");
	VerticesHashMap=new HashMap<String,String>();	
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
        	     VerticesHashMap.put(str[0],"");
                 line=br.readLine();
         	}
		
 	}
	}catch(Exception e)
	{
		Log.info(e.getMessage());
	}
		
	}
	
	
	//INPUT:Output of previous job
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String[] data=value.toString().split("\\s+");
		String ClosedAdjList="";
		
		for(String neighbour:data[1].split(",")){
			
			if(VerticesHashMap.containsKey(neighbour)){
				
				ClosedAdjList+=","+neighbour;
				
			}
			
		}
		
		if(ClosedAdjList.length()>0)
		{
		context.write(new Text(data[0]), new Text(ClosedAdjList.substring(1,ClosedAdjList.length())));
		}
		else{
			context.write(new Text(data[0]), new Text(ClosedAdjList));	
		}
	
		
		
	}
}