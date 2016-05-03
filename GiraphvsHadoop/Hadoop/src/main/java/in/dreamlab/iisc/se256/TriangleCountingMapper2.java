package in.dreamlab.iisc.se256;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class TriangleCountingMapper2 extends Mapper<Object , Text, Text, Text> {

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
	   
		String data=value.toString();
		if(!data.contains("#") && !data.contains("c") && !data.contains("p")  )
		{	
		if(data.contains(","))
		{
			String[] TwoPaths=data.split("\\s+");
			context.write(new Text(TwoPaths[1]), new Text(TwoPaths[0]));
		}
		else
		{
			String[] nodes=data.split("\\s+");
			if(Long.valueOf(nodes[0]) < Long.valueOf(nodes[1]))
			context.write(new Text(nodes[0]+"," + nodes[1]), new Text("$"));
		}
		}
		
	}
}