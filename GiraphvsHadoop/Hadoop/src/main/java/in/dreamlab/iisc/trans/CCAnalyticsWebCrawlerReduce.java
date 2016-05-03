package in.dreamlab.iisc.trans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
//import in.ac.iisc.cds.se256.alpha.CC.pojo.IntTextPairWritable;

public class CCAnalyticsWebCrawlerReduce
{
	private static final Logger LOG = Logger.getLogger(CCAnalyticsWebCrawlerReduce.class);
	
	protected static enum REDUCERCOUNTER 
	{
		REDUCER_WRITE,
		EXCEPTIONS
	}
	
	protected static class CCAnalyticsWebCrawlerReducer extends Reducer<Text,Text,Text,Text>
	{
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{

try{
	for(Text t:values){
		context.write(key,new Text(""));
		break;
	}

}
catch (Exception e){}

		}


	}}
