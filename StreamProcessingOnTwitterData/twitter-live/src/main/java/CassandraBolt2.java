import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class CassandraBolt2 extends BaseRichBolt{
	private static final Logger LOG = Logger.getLogger(CassandraBolt2.class);
	private OutputCollector collector;
	private Map<String,Long> sentimentMap;
	private Cassandra client;
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		 this.collector=collector;
		 this.sentimentMap=new HashMap<String,Long>();
		client=new Cassandra();
		client.connect("127.0.0.1");
		client.createSchema();
	}
	public void execute(Tuple input) {
		String country=input.getStringByField("country");
		long total=input.getLongByField("total");
		client.loadData(country,total);
	}
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields());
	}
}