package in.dreamlab.iisc.se256;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		
		Configuration conf = new Configuration();
		Path Input_PATH=new Path(args[0]);
		Path Output_PATH=new Path(args[1]);
		
		Job job1 = Job.getInstance(conf, "conf1");
           
		job1.setJarByClass(WordMapper.class);
	    job1.setMapperClass(WordMapper.class);

	    job1.setReducerClass(WordReducer.class);

	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(IntWritable.class);

	    job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);
		
	    FileInputFormat.addInputPath(job1, Input_PATH);
	    FileOutputFormat.setOutputPath(job1, Output_PATH);

	    //TextInputFormat.addInputPath(job1);
		System.exit(job1.waitForCompletion(true) ? 0 : 1);
		
	
	
	}
}
