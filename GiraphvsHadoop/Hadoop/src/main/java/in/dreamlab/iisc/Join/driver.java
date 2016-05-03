package in.dreamlab.iisc.Join;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.util.Map;

public class driver {


    public static void main(String[] args) throws Exception {


        String inputPath = args.length >= 1 ? args[0] : "/SE256/TWITTER/twitter_rv.net";
        String outputPath = args.length >= 2 ? args[1] : "alpha/twitter/output";
        long numMaps = Long.parseLong(args.length >= 3 ? args[2] : "2"); // not used now
        long numReduces = Long.parseLong(args.length >= 4 ? args[3] : "2");
        long memMaps = Long.parseLong(args.length >= 5 ? args[4] : "4000");
        long memReduces = Long.parseLong(args.length >= 6 ? args[5] : "4000");
        String i = args[6];
        String file_path = args[7];


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
        conf.set("i", i);
        conf.set("file_name", file_path);

        /////////////////////////////////////////////////////////
        // Init job with configuration. Update configuration BEFORE creating job.
        // Give a human-friendly job name.
        Job job = Job.getInstance(conf, "file join");


        // Log configuration used to run job



        /////////////////////////////////////////////////////////
        // Set classes for Job, Mapper, Combiner, Partitioner, Reducer.
        job.setJarByClass(driver.class);
        job.setReducerClass(MyReducer.class);
        //job.setCombinerClass(TwitterTopoCounterCombiner.class);
        job.setMapperClass(MapperState.class);
        //job.setPartitionerClass(TwitterTopoCounterPartitioner.class);


        /////////////////////////////////////////////////////////
        // Set input and output file locations.
        // NOTE: Input has to be under /user/$username (or) /SE256
        FileInputFormat.addInputPath(job, new Path(inputPath));

        // NOTE: Output has to be under /user/$username ONLY. Not having a leading '/' will ensure that.
		/*FileSystem fs = FileSystem.newInstance(conf);
		if (fs.exists(new Path(outputPath))) { // delete output folder if already present
			fs.delete(new Path(outputPath), true);
		}*/
        FileOutputFormat.setOutputPath(job, new Path(outputPath));


        /////////////////////////////////////////////////////////
        // Set input and output file formats.
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        /////////////////////////////////////////////////////////
        // Set output Key-Value types.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        // Submit the job, then poll for progress until the job is complete
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}