package in.dreamlab.iisc.se256;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;



public class HashTagGenerationReducer extends Reducer<Text,Text,Text, Text> {
	
	HashMap<String,String> VerticesHashTagsMap;
	
	 MultipleOutputs<Text, Text> mos;
	 
	 String iteration;
	@Override
	protected void setup(Context context){
		mos = new MultipleOutputs(context);
		iteration=context.getConfiguration().get("iteration");
		if(!iteration.equals("0"))
		{
			try
			{
			
			
			//Initializing HashTags of each user
			//"hdfs://sslcluster:9000/SE256/ASGNB/se256-beta-JOB1/IdMap"
			VerticesHashTagsMap=new HashMap<String,String>();
			FileSystem fs = FileSystem.get(new Configuration());//Change hdfs path accordingly
			String currentHashMapPath=context.getConfiguration().get("HashMapPath");
			FileStatus[] status = fs.listStatus(new Path("hdfs://sslcluster:9000/" +currentHashMapPath));//change this accordingly
			for (int i=0;i<status.length;i++){
				if(status[i].getPath().toString().contains("VerticesHashMap"))
				{
					BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
                 	String line;
                 	line=br.readLine();
                 	while (line != null){
//                
                	     String[] str=line.split("\\s+");
                	     VerticesHashTagsMap.put(str[0], str[1]);
                         line=br.readLine();
                 	}
				}
			}
			}catch(Exception e)
			{
			System.out.println(e.getMessage());
			}
		}
		
		
	}
	
	
	@Override
	protected void reduce(Text key, Iterable<Text> values , Context context)
			throws IOException, InterruptedException {
		
		
		String currentVertex=key.toString();
		String hashTags="";
		for(Text v:values){
			hashTags=v.toString();
			break;
		}
//		System.out.println("##################"+hashTags);
		if(!iteration.equals("0"))
		{
		String TopHashTags=String.valueOf(hashTags);
			int num_hashTags=TopHashTags.split("\\$").length;
			String[] prevHashTags=VerticesHashTagsMap.get(currentVertex).split("\\$");
			int num_retain=10-num_hashTags;

			
			if(prevHashTags.length<num_retain){
				num_retain=prevHashTags.length;
			}
			
//			System.out.println("Adding HashTags"+num_hashTags + "Retaining" + num_retain );
			for(int i=0;i<num_retain;i++){
				
				
				
					TopHashTags=TopHashTags + "$" + prevHashTags[i];
				
				
			}
	 //for top 10 hashtags
      VerticesHashTagsMap.put(currentVertex, TopHashTags);
      mos.write("VerticesHashMap", key, TopHashTags);
    
		}
		else
		{
			 mos.write("VerticesHashMap",key, new Text(hashTags));
		}
      //for Timestep
      mos.write("TimeStep",key, new Text(hashTags));
      
	
	
	}
	
	
	
	@Override
	protected void cleanup(Context context){
		try
		{
	mos.close();
		}
		catch(Exception e){
			
		}
		
	}
	
	

}