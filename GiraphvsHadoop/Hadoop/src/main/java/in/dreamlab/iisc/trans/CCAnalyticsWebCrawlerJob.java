package in.dreamlab.iisc.trans;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URL;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
//import org.commoncrawl.warc.WARCFileInputFormat;

import in.dreamlab.iisc.trans.CCAnalyticsWebCrawlerReduce.CCAnalyticsWebCrawlerReducer;
import in.dreamlab.iisc.trans.CCAnalyticsWebCrawlerMap.CCAnalyticsWebCrawlerMapper;
//import in.ac.iisc.cds.se256.alpha.cc.CCAnalyticsWebCrawlerCombine.CCAnalyticsWebCrawlerCombiner;
//import in.ac.iisc.cds.se256.alpha.cc.pojo.IntTextPairWritable;


public class CCAnalyticsWebCrawlerJob extends Configured implements Tool
{
	private static final Logger LOG = Logger.getLogger(CCAnalyticsWebCrawlerJob.class);

	public static void main(String[] args) throws Exception 
	{
		int res = ToolRunner.run(new Configuration(), new CCAnalyticsWebCrawlerJob(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception 
	{
		/////////////////////////////////////////////////////////
		// Parse args from commandline
		// NOTE: Input has to be under /user/$username (or) /SE256 
		// NOTE: Output has to be under /user/$username ONLY. Not having a leading '/' will ensure that.

		//String inputPath = args.length >= 1 ? args[0] : "/SE256/CC/";
		//String outputPath = args.length >= 2 ? args[1] : "alpha/cc/output";
		long numMaps = Long.parseLong(args.length >= 3 ? args[2] : "44"); // not used now
		long numReduces = Long.parseLong(args.length >= 4 ? args[3] : "1");
		long memMaps = Long.parseLong(args.length >= 5 ? args[4] : "4000");
		long memReduces = Long.parseLong(args.length >= 6 ? args[5] : "15000");






		/////////////////////////////////////////////////////////
		// Init configuration. Do this BEFORE creating Job instance. 
		// Set Mapper and Reducer resource requirements, overriding default.
		Configuration conf = new Configuration();

		//		conf.setBoolean("yarn.nodemanager.vmem-check-enabled", true);
		//		conf.setBoolean("yarn.nodemanager.pmem-check-enabled", true);
		//		conf.setDouble("yarn.nodemanager.vmem-pmem-ratio", 1.5);

		conf.setLong("mapreduce.map.memory.mb", memMaps); // we have large input compressed files, so mapper needs more memory 
		conf.setLong("mapreduce.map.java.opts.max.heap", (long)0.9*memMaps); // ensure we don't go to virtual memory
		conf.setLong("mapreduce.map.cpu.vcores", 1); // one core will suffice for each mapper
		//conf.setLong("mapreduce.job.maps", numMaps); // number of mappers decided by splits from WARC input format  

		conf.setLong("mapreduce.reduce.memory.mb", memReduces); // Reducer output is more modest
		conf.setLong("mapreduce.reduce.java.opts.max.heap", (long)0.9*memReduces);
		conf.setLong("mapreduce.reduce.cpu.vcores", 1);
		conf.setLong("mapreduce.job.reduces", numReduces);
		int iterations = new Integer(args[6]);
		String vertex=new String(args[7]);
		String file_name;
		Path inPath = new Path(args[0]);
		Path outPath = null;
		for (int i = 0; i < iterations; ++i) {
			conf.set("i", String.valueOf(i));
			if(i==0){

				conf.set("vertex", vertex);
			}
			outPath = new Path(args[1] + i);
			Job job = new Job(conf, "transitive closure");
			// Set classes for Job, Mapper, Combiner, Partitioner, Reducer.
			job.setJarByClass(CCAnalyticsWebCrawlerJob.class);
			job.setMapperClass(CCAnalyticsWebCrawlerMapper.class);
			//job.setCombinerClass(CCAnalyticsWebCrawlerCombine.CCAnalyticsWebCrawlerCombiner.class);
			job.setReducerClass(CCAnalyticsWebCrawlerReducer.class);
			//job.setPartitionerClass(CCAnalyticsWebCrawlerPartition.class);


			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, inPath);
			FileOutputFormat.setOutputPath(job, outPath);
			job.waitForCompletion(true);
			file_name=outPath.toString();
			conf.set("file_name",file_name.substring(1,file_name.length()));
			//inPath = outPath;
		}
		//conf.set("whether",iterfirst);

		//conf.setStrings("mapping",tempsArray);
		//String token1= "";
		//Scanner inFile1 = new Scanner(new File("/home/secprav/stopwords.txt")).useDelimiter(",\\s*");
		//Scanner inFile1 = new Scanner(new File("/home/prateeksha/Desktop/stopwords.txt")).useDelimiter(",\\s*");


		// Original answer used LinkedList, but probably preferable to use ArrayList in most cases
		// List<String> temps = new LinkedList<String>();





		/////////////////////////////////////////////////////////
		// Init job with configuration. Update configuration BEFORE creating job.
		// Give a human-friendly job name.




		/////////////////////////////////////////////////////////
		// Set classes for Job, Mapper, Combiner and Reducer. 
		/*crawlingJob.setJarByClass(CCAnalyticsWebCrawlerJob.class);
		crawlingJob.setMapperClass(CCAnalyticsWebCrawlerMapper.class);
		crawlingJob.setCombinerClass(CCAnalyticsWebCrawlerCombine.CCAnalyticsWebCrawlerCombiner.class);
		crawlingJob.setReducerClass(CCAnalyticsWebCrawlerReducer.class);*/



		/////////////////////////////////////////////////////////
		// Set input and output file locations. 
		// NOTE: Input has to be under /user/$username (or) /SE256 



		/////////////////////////////////////////////////////////


		// Submit the job, then poll for progress until the job is complete
		//return crawlingJob.waitForCompletion(true) ? 0 : 1;
		return 1;
	}

}
