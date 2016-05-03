package in.dreamlab.iisc.hashtag;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import javax.security.auth.login.Configuration;

/**
 * 
 * @author simmhan
 *
 */
public class TwitterTopoCounterReduce {
	private static final Logger LOG = Logger.getLogger(TwitterTopoCounterReduce.class);
	protected static enum REDUCERCOUNTER {
		EDGES, VERTICES,
		EXCEPTIONS
	}
	

	protected static class TwitterTopoCounterReducer extends Reducer<Text,Text, Text, Text> {
		String hashtag;



		public void reduce(Text key, Iterable<Text> values, Context context) {
			try{
				long sum=0;
for(Text t:values){
	sum=sum+Long.parseLong(t.toString());
}
				context.write(new Text(key),new Text(String.valueOf(sum)));

			}
			catch (Exception ex) {
				LOG.error("Caught Exception", ex);
				context.getCounter(REDUCERCOUNTER.EXCEPTIONS).increment(1);
			}
		}


		}
	}	

