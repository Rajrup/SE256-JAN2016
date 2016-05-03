package in.dreamlab.iisc.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

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
	
	/**
	 * Reduce function that counts the number of vertices and edges 
	 * @author simmhan
	 *
	 */
	protected static class TwitterTopoCounterReducer extends Reducer<Text,Text, Text,Text> {




		
		@Override
	    public void reduce(Text key, Iterable<Text> values, Context context) {
			int id=context.getTaskAttemptID().getTaskID().getId();


			try{
				String str;
				double sum=0.0;
				String adjacency_list="";
				String page_rank;
				int flag=0;



				for(Text t:values){

					str=t.toString().trim();
					if(str.charAt(str.length() - 1)=='$'){
						flag=1;
						page_rank=str.substring(0,str.indexOf('$'));
						sum=sum+Double.parseDouble(page_rank);
					}
					else{
						adjacency_list=t.toString().trim();
					}



				}
				if(flag==0){
					sum=0.0;
				}
				sum=0.85*sum+0.15*(1.0/3997962.0);
				context.write(key,new Text(adjacency_list+" "+String.valueOf(sum)));
				context.write(new Text("id"),new Text(String.valueOf(id)));



			}
			catch (Exception ex) {
				//LOG.error("Caught Exception value of str"+check, ex);
				context.getCounter(REDUCERCOUNTER.EXCEPTIONS).increment(1);
			}
		}


	}	
}
