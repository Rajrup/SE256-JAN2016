package in.dreamlab.iisc.se256;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class WordMapper extends Mapper<Object , Text, Text, IntWritable> {

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		
		String line=value.toString();
		//System.out.println("TESTM: word is "+line);
		String[] strs = line.trim().split("\\s+");

		for(int i=0;i<strs.length;i++){
			
			//System.out.println("TESTM: word is "+strs[i]);
			
			context.write(new Text(strs[i]), new IntWritable(1));
			
		}
		
		
	}
}