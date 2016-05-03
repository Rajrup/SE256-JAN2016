import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
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


public class TwitterFileSpout extends BaseRichSpout{
	private static final Logger LOG = Logger.getLogger(TwitterFileSpout.class);
	private String fileName;
	private SpoutOutputCollector collector;
	private ObjectInputStream ois;
	public TwitterFileSpout(String fileName){
		this.fileName=fileName;
	}
	private boolean alive;
	private static final long serialVersionUID = 1L;
	
	
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		Configuration configuration = new Configuration();
		BufferedInputStream bis;
		LOG.info("open spout");
		try {
//			FileSystem fs = FileSystem.get(new URI("hdfs://localhost:54310"), configuration);
//			
//			Path pt = new Path(fileName);
			
			//FSDataInputStream fsDataInputStream = fs.open(pt);
			FileInputStream fsDataInputStream=new FileInputStream(fileName);
			
			
			bis=new BufferedInputStream(fsDataInputStream);
			ois=new ObjectInputStream(bis);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.collector=collector;
		alive=true;
	}
	public void nextTuple() {
		if(alive){
			try {
					Status status=(Status) ois.readObject();
					//LOG.info("status is: "+status.getText());
					collector.emit(new Values(status));
			}catch(EOFException e){
				//timeStop=System.currentTimeMillis();
				//LOG.info("Time taken is:"+(timeStop-timeStart)+"(ms)");
				alive=false;
			}
			catch (ClassNotFoundException e) {
				LOG.error("Exception: "+e);
				alive=false;
			} catch (IOException e) {
				LOG.error("Exception: "+e);
				alive=false;
			}
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("status"));
	}

}
