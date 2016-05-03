import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;


import twitter4j.Status;


public class NormalFileSpout extends BaseRichSpout{
	private static final Logger LOG = Logger.getLogger(NormalFileSpout.class);
	private String fileName;
	private SpoutOutputCollector collector;
	private ObjectInputStream ois;
	public NormalFileSpout(String fileName){
		this.fileName=fileName;
	}
	private boolean alive;
	private static final long serialVersionUID = 1L;
	private BufferedReader bis;
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		Configuration configuration = new Configuration();
		
		LOG.info("open spout");
		try {
			//FileSystem fs = FileSystem.get(configuration);
			//Path pt =new Path("hdfs://localhost:54310"+fileName);
			FileSystem fs = FileSystem.get(new URI("hdfs://localhost:54310"), configuration);
			Path pt = new Path(fileName);
			FSDataInputStream fsDataInputStream = fs.open(pt);
			bis=new BufferedReader(new InputStreamReader(fsDataInputStream));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.collector=collector;
		alive=true;
	}

	public void nextTuple() {
		if(alive){
			try {
				StringBuilder builder=new StringBuilder();
				String line;
				int count=0;
				int continueCount=0;
				while(true){
					line=bis.readLine();
					if(line!=null)
						line=line.replaceAll("“|”", "");
					builder.append(line);
					if(continueCount>10)
						break;
					if(line==null||line.length()==0){
						continueCount++;
						continue;
					}
					continueCount=0;
					count+=line.length();
					if((line.charAt(line.length()-1)=='.')||(count>1000))
						break;
				}
				collector.emit(new Values(builder.toString()));
					
			}catch(EOFException e){
				
			}
			catch (IOException e) {
				LOG.error("Exception: "+e);
				alive=false;
			}
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
	}

}
