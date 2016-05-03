package in.dreamlab.iisc.pagerank;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import com.sun.tools.javac.util.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.log4j.Logger;

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
		private HashMap mappingHm=new HashMap();
		int i;
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf=context.getConfiguration();
			i=conf.getInt("i", 0);

			Path pt=new Path("hdfs://sslcluster:9000/user/prateeksha/adjlivejournalhadoop.txt");//Location of file in HDFS
			//Path pt=new Path("hdfs://localhost:9000/user/input1/mapping22.txt");
			//Path pt=new Path("hdfs://orion-00:9000/prateeksha/mapping.txt");

			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
			String line;
			List<String> list = new ArrayList<String>();

			line=br.readLine();
			while (line != null){
				list.add(line);
				line=br.readLine();
			}

			String[] mapping = list.toArray(new String[0]);
			String url;
			String id;
			String url_id[];


			for(int i=0;i<mapping.length;i++)
			{

				url_id=mapping[i].split("\\s+");
				id=url_id[0].trim();
				//url=url_id[1].trim();


				mappingHm.put(id,0);



			}
		}



		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException {
			try{
				String[] vertexPair = value.toString().split("\\s+");
				String source_vertex=vertexPair[0].trim();
				String list_of_sink_vertex[]=vertexPair[1].split(",");
				int number_of_sink_vertices=0;
				for(int i=0;i<list_of_sink_vertex.length;i++){
					if(!list_of_sink_vertex[i].trim().equals(" ")&&!list_of_sink_vertex[i].trim().equals("")){
						number_of_sink_vertices++;
					}


				}
				//String pagerank;
				if(vertexPair.length==2){
					double page_rank=1.0/number_of_sink_vertices;
					for(int i=0;i<list_of_sink_vertex.length;i++){
						if(!list_of_sink_vertex[i].equals(" ")&&!list_of_sink_vertex[i].equals("")&&!list_of_sink_vertex[i].equals("-1"))
						{

							context.write(new Text(list_of_sink_vertex[i].trim()),new Text(String.valueOf(page_rank)+"$"));
						}
					}

				}
				
				else{
					double page_rank=Double.parseDouble(vertexPair[2].trim());
					if(list_of_sink_vertex.length==1&&list_of_sink_vertex[0]=="-1"){
						Set setOfKeys = mappingHm.keySet();
						Iterator iterator = setOfKeys.iterator();


/**
 * Loop the iterator until we reach the last element of the HashMap
 */if(i!=1&&i%10==0){
						while (iterator.hasNext()) {
/**
 * next() method returns the next key from Iterator instance.
 * return type of next() method is Object so we need to do DownCasting to String
 */
							String ke = (String) iterator.next();

          context.write(new Text(ke),new Text(String.valueOf(page_rank/3997962.0)+"$"));
 /* once we know the 'key', we can get the value from the HashMap
 * by calling get() method
 */
							//Integer value = (Integer)hmap.get(key);

							//System.out.println("Key: "+ key+", Value: "+ value);}


						}}
					}


                    page_rank=page_rank/number_of_sink_vertices;
					for(int i=0;i<list_of_sink_vertex.length;i++){
						if(!list_of_sink_vertex[i].equals(" ")&&!list_of_sink_vertex[i].equals("")&&!list_of_sink_vertex[i].equals("-1"))
						{context.write(new Text(list_of_sink_vertex[i].trim()),new Text(String.valueOf(page_rank)+"$"));
						}
					}


				}
				context.write(new Text(source_vertex.trim()),new Text(vertexPair[1]));
			}
			catch (Exception ex) {
				LOG.error("Caught Exception", ex);
				context.getCounter(MAPPERCOUNTER.EXCEPTIONS).increment(1);
			}
		}
	}
	

}
