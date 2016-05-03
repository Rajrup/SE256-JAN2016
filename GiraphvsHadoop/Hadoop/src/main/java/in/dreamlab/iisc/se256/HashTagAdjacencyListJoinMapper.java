package in.dreamlab.iisc.se256;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.mortbay.log.Log;


public class HashTagAdjacencyListJoinMapper extends Mapper<Object , Text, Text, Text> {

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
			String[] data=value.toString().split("\\s+");
		
		
		
			
			context.write(new Text(data[0]), new Text(data[1]));
			
			
			
		

		
		
	}
}