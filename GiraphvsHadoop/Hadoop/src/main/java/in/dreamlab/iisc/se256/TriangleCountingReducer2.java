package in.dreamlab.iisc.se256;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class TriangleCountingReducer2 extends Reducer<Text,Text,Text, Text> {
	
	@Override
	protected void reduce(Text key, Iterable<Text> values , Context context)
			throws IOException, InterruptedException {
		int count=0;
		boolean flag=false;
		ArrayList<String> nodes=new ArrayList<String>();
		for(Text v:values){
			if(v.toString().contains("$"))
				flag=true;
			else{
				count++;
				nodes.add(v.toString());
			}
				
		}
		
		if(flag==true && count > 0){
			context.write(key, new Text(count + ""));
		}
		
	}

}