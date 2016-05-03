package in.dreamlab.iisc.hashtag;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.log4j.Logger;

import javax.security.auth.login.Configuration;

/**
 * 
 * @author simmhan
 *
 */
public class TwitterTopoCounterMap {
	private static final Logger LOG = Logger.getLogger(TwitterTopoCounterMap.class);
	protected static enum MAPPERCOUNTER {
		RECORDS_IN,
		EXCEPTIONS
	}
	
	/**
	 * Map function that counts the number of vertices and edges 
	 * @author simmhan
	 *
	 */
	protected static class TwitterTopoCounterMapper extends Mapper<LongWritable, Text, Text, Text> {
     long count;
		String hashtag;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			count=0;
org.apache.hadoop.conf.Configuration conf=context.getConfiguration();
			hashtag=conf.get("hashtag");

		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException {
			try{
                int localcount=0;
				String data[]=value.toString().split("\\s+");
				String vertex=data[0].trim();
				String hashtags[]=data[1].split("\\$");
				for(int i=0;i<hashtags.length;i++){
					hashtags[i]=hashtags[i].split(":")[0].trim().toLowerCase();
					if(hashtags[i].equals(hashtag.toLowerCase())){
						count++;
						localcount++;
					}
				}
                context.write(new Text(vertex),new Text(String.valueOf(localcount)));
			}
			catch (Exception ex) {
				LOG.error("Caught Exception", ex);
				context.getCounter(MAPPERCOUNTER.EXCEPTIONS).increment(1);
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			context.write(new Text(hashtag),new Text(String.valueOf(count)));
		}
	}
	

}
