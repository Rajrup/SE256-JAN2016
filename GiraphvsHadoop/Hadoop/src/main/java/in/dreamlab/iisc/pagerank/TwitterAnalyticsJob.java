package in.dreamlab.iisc.pagerank;

import in.dreamlab.iisc.pagerank.TwitterTopoCounterMap.TwitterTopoCounterMapper;
//import in.ac.iisc.cds.se256.alpha.twitter.TwitterTopoCounterMap.TwitterTopoCounterPartitioner;
import in.dreamlab.iisc.pagerank.TwitterTopoCounterReduce.TwitterTopoCounterReducer;
//import in.ac.iisc.cds.se256.alpha.twitter.solution.TwitterTopoCounterCombine.TwitterTopoCounterCombiner;

import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * Based on by WARCTagCounter by Stephen Merity (Smerity)
 * 
 * @author simmhan
 *
 */
public class TwitterAnalyticsJob extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(TwitterAnalyticsJob.class);
 	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new TwitterAnalyticsJob(), args);
        System.exit(res);
	}
		
	public int run(String[] args) throws Exception {
		/////////////////////////////////////////////////////////
		// Parse args from commandline
		// NOTE: Input has to be under /user/$username (or) /SE256
		// NOTE: Output has to be under /user/$username ONLY. Not having a leading '/' will ensure that.
		//String inputPath = args.length >= 1 ? args[0] : "/SE256/TWITTER/twitter_rv.net";
		//String outputPath = args.length >= 2 ? args[1] : "alpha/twitter/output";
		long numMaps = Long.parseLong(args.length >= 3 ? args[2] : "2"); // not used now
		long numReduces = Long.parseLong(args.length >= 4 ? args[3] : "2");
		long memMaps = Long.parseLong(args.length >= 5 ? args[4] : "4000");
		long memReduces = Long.parseLong(args.length >= 6 ? args[5] : "4000");


		/////////////////////////////////////////////////////////
		// Init configuration. Do this BEFORE creating Job instance.
		// Set Mapper and Reducer resource requirements, overriding default.
		Configuration conf = new Configuration();

		conf.setLong("mapreduce.map.memory.mb", memMaps); // we have large input compressed files, so mapper needs more memory 
		conf.setLong("mapreduce.map.java.opts.max.heap", (long) 0.9 * memMaps); // ensure we don't go to virtual memory
		conf.setLong("mapreduce.map.cpu.vcores", 1); // one core will suffice for each mapper
		//conf.setLong("mapreduce.job.maps", numMaps); // number of mappers decided by splits from input format  

		conf.setLong("mapreduce.reduce.memory.mb", memReduces); // Reducer output is more modest
		conf.setLong("mapreduce.reduce.java.opts.max.heap", (long) 0.9 * memReduces);
		conf.setLong("mapreduce.reduce.cpu.vcores", 1);
		conf.setLong("mapreduce.job.reduces", numReduces);
		int iterations = new Integer(args[6]);
		Path inPath = new Path(args[0]);
		Path outPath = null;
		for (int i = 1; i <= iterations; ++i) {
			conf.setInt("i",i);
			outPath = new Path(args[1] + i);
			Job job = new Job(conf, "page rank");
			// Set classes for Job, Mapper, Combiner, Partitioner, Reducer.
			job.setJarByClass(TwitterAnalyticsJob.class);
			job.setMapperClass(TwitterTopoCounterMapper.class);
			//job.setCombinerClass(TwitterTopoCounterCombiner.class);
			job.setReducerClass(TwitterTopoCounterReducer.class);


			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, inPath);
			FileOutputFormat.setOutputPath(job, outPath);
			job.waitForCompletion(true);
			inPath = outPath;
		}


		/////////////////////////////////////////////////////////
		// Init job with configuration. Update configuration BEFORE creating job.
		// Give a human-friendly job name. 
		//Job job = Job.getInstance(conf, "Twitter Analytics by Hobbes v1");

		// Log configuration used to run job


		/////////////////////////////////////////////////////////

		// job.setPartitionerClass(TwitterTopoCounterPartitioner.class);


		/////////////////////////////////////////////////////////
		// Set input and output file locations.
		// NOTE: Input has to be under /user/$username (or) /SE256
		/*FileInputFormat.addInputPath(job, new Path(inputPath));

	    // NOTE: Output has to be under /user/$username ONLY. Not having a leading '/' will ensure that.
		FileSystem fs = FileSystem.newInstance(conf);
		if (fs.exists(new Path(outputPath))) { // delete output folder if already present
			fs.delete(new Path(outputPath), true);
		}
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		LOG.info("Input path: " + inputPath);
		LOG.info("Output path: " + outputPath);


	    /////////////////////////////////////////////////////////
	    // Set input and output file formats.
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);


	    /////////////////////////////////////////////////////////
	    // Set output Key-Value types.
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);

	    
	    // Submit the job, then poll for progress until the job is complete
           */return 1;
	}

}
