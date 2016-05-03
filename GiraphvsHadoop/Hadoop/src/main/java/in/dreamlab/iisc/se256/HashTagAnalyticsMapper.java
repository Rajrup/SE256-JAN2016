package in.dreamlab.iisc.se256;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class HashTagAnalyticsMapper extends Mapper<Object , Text, Text, Text> {

	String Tag;
	
	@Override
	protected void setup(Context context){
		
		Tag="popularphoto";
	}
	
	
	//INPUT:Vertex HashTagList AdjacencyList
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		
	String[] data=value.toString().split("\\s+");
	String[] HashTags=data[1].split("\\$");
	
	boolean flag=false;
	for(String hashTag:HashTags)
	{
		if(hashTag.equals(Tag)){
			flag=true;
			break;
		}
	}
	
	if(flag==true){
		
		context.write(new Text(data[0]), new Text(data[2]));
	}
		
		
	}
}