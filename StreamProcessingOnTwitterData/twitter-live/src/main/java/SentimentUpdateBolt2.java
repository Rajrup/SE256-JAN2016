
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


public class SentimentUpdateBolt2 extends BaseRichBolt{
	private static final Logger LOG = Logger.getLogger(SentimentUpdateBolt2.class);
	private OutputCollector collector;
	private Map<String,Long> totalMap;
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		 this.collector=collector;
		 this.totalMap=new HashMap<String,Long>();
	}
	public void execute(Tuple input) {
		String country=input.getStringByField("country");
		long totalSentimentLong=0;
		Long total=totalMap.get(country);
		if(total==null)
		{
			totalSentimentLong=1;
		}else{
			totalSentimentLong=1+total.longValue();
		}
		totalMap.put(country, new Long(totalSentimentLong));
		collector.emit(new Values(country,totalSentimentLong));
		LOG.info("Sentiment count of "+country+" is "+totalSentimentLong);
	}
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("country","total"));
	}
}
