package in.dreamlab.iisc.se256;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class WordReducer extends Reducer<Text,IntWritable,Text, IntWritable> {
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values , Context context)
			throws IOException, InterruptedException {
		
		
		
		int count=0; 
		for(IntWritable v : values){
			count=count+v.get();
//			adjlist.add(v.get());
		}

		//System.out.println("TESR: for Key "+key.toString()+" count is "+count);
		
		context.write(key, new IntWritable(count));	
	}

}