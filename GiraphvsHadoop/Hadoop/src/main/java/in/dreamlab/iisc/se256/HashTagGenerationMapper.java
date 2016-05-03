package in.dreamlab.iisc.se256;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.mortbay.log.Log;

public class HashTagGenerationMapper extends Mapper<Object , Text, Text, Text> {

	double probability_new;
	double probability_old;
	double probability_friends;
	int hash_per_timestep;
	
	HashMap<String,String> VerticesHashTagsMap;
	String[] arr;
	String iteration;

	@Override
	protected void setup(Context context){
		//get iteration number
		iteration=context.getConfiguration().get("iteration");
		
		
		//HyperParameters initialize it accordingly
		probability_new=0.3;
		probability_old=0.2;
		probability_friends=0.5;
		hash_per_timestep=5;
		
		//Initializing hashtagslist
		arr=context.getConfiguration().getStrings("hash_tags");
		VerticesHashTagsMap=new HashMap<String,String>();
		if(!iteration.equals("0"))
		{	
		
			String hashMapPath=context.getConfiguration().get("HashMapPath");
			
			try
			{
			//Initializing HashTags of each user
			
		 	FileSystem fs = FileSystem.get(new Configuration());//Change hdfs path accordingly
         	FileStatus[] status = fs.listStatus(new Path("hdfs://sslcluster:9000/" +hashMapPath));
         	for (int i=0;i<status.length;i++){
         		if(status[i].getPath().toString().contains("VerticesHashMap"))
				{
//         			System.out.println("***********************" + status[i].getPath().toString());
         			BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
                 	String line;
                 	line=br.readLine();
                 	while (line != null){
//                	
                	     String[] str=line.split("\\s+");
                	     VerticesHashTagsMap.put(str[0], str[1]);
//                	     Log.info(str[0] + ":" + str[1]);
//                	     System.out.println("Value******************** "+str[0] +":" +VerticesHashTagsMap.get(str[0]));
                         line=br.readLine();
                 	}
				}
         	}
			}catch(Exception e)
			{
				Log.info(e.getMessage());
			}
		}
		
		
		
	}
	
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String[] data=value.toString().split("\\s+");
		Random randomHashTagIndex = new Random();
		Random randomNumHashTags = new Random();
		Random randomChooseNeighbour = new Random();
		Random randomNewOldFriend = new Random();
		
		int num=randomNumHashTags.nextInt(6);
		
		while(num==0){
			num=randomNumHashTags.nextInt(6);
		}
		
		String hashTag="";
		for(int i=0;i<num;i++){
			
			
			int bucket=randomNewOldFriend.nextInt(100);
			int HashTagType=0;
			if(bucket<probability_new*100){
				HashTagType=0;
			}
			else if(bucket<(probability_new + probability_old)){
				HashTagType=1;
				
			}else{
				HashTagType=2;
				if(data[1].equals("-1")){
					HashTagType=0;
				}
			}
				//change case 1 and 2 to incorporate distribution
			if(!iteration.equals("0"))
			{
				switch(HashTagType){
				case 0:
					int index=randomHashTagIndex.nextInt(arr.length);
					if(hashTag.equals(""))
					hashTag=arr[index] + ":" +iteration;
					else{
						hashTag=hashTag+"$" +arr[index] + ":" + iteration;
					}
				
					break;
				
				case 1:
					
					String[] OwnHashTag_Timestep=VerticesHashTagsMap.get(data[0]).split("\\$");
					int OwnIndex=randomHashTagIndex.nextInt(OwnHashTag_Timestep.length);
					String OwnHashTag=OwnHashTag_Timestep[OwnIndex].split(":")[0];
					if(hashTag.equals(""))
						hashTag=OwnHashTag + ":" +iteration;
						else{
							hashTag=hashTag+"$" +OwnHashTag + ":" + iteration;
						}
					break;
				
				case 2:
					if(!data[1].equals("-1"))
					{
					String[] neighbours=data[1].split(",");
					int neighbourIndex=randomChooseNeighbour.nextInt(neighbours.length);
					String neighbour=neighbours[neighbourIndex];
					String[] NeighbourHashTag_Timestep=VerticesHashTagsMap.get(neighbour).split("\\$");
					int HashTagIndex=randomHashTagIndex.nextInt(NeighbourHashTag_Timestep.length);
					String NeighbourHashTag=NeighbourHashTag_Timestep[HashTagIndex].split(":")[0];
					if(hashTag.equals(""))
						hashTag=NeighbourHashTag + ":" +iteration;
						else{
							hashTag=hashTag+"$" +NeighbourHashTag+ ":" + iteration;
						}
					}
					break;
			
				}
				
				
			}else
			{
				int index=randomHashTagIndex.nextInt(arr.length);
				if(hashTag.equals(""))
				hashTag=arr[index] + ":" +iteration;
				else{
					hashTag=hashTag+"$" +arr[index] + ":" + iteration;
				}
			}
			
		}
		
		context.write(new Text(data[0]), new Text(hashTag));
	}
	
	
	
}