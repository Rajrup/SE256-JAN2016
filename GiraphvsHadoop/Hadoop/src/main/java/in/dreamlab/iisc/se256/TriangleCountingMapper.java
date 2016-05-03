package in.dreamlab.iisc.se256;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class TriangleCountingMapper extends Mapper<Object , Text, Text, Text> {

	
	//INPUT FORMAT: Source Destination
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String line=value.toString();
		if(!line.contains("#") && !line.contains("c") && !line.contains("p"))
		{	
		String[] nodes=line.split("\\s+");
		
		if(Long.valueOf(nodes[0]) < Long.valueOf(nodes[1]))
		{
//			System.out.println(nodes[0] +" " + nodes[1]);
		context.write(new Text(nodes[0]), new Text(nodes[1]));
		}
		
		}
	}
}