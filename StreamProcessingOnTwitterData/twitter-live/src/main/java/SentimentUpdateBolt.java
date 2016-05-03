
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


public class SentimentUpdateBolt extends BaseRichBolt{
	private static final Logger LOG = Logger.getLogger(SentimentUpdateBolt.class);
	private OutputCollector collector;
	private Map<String,Long> sentimentMap;
	private Map<String,Long> totalMap;
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		 this.collector=collector;
		 this.sentimentMap=new HashMap<String,Long>();
		 this.totalMap=new HashMap<String,Long>();
	}
	private int count=0;
	public void execute(Tuple input) {
		int sentiment=input.getIntegerByField("sentiment");
		String country=input.getStringByField("country");
		long totalSentimentLong=0;
		long valueSentimentLong=0;
		Long current=sentimentMap.get(country);
		Long total=totalMap.get(country);
		if(current==null)
		{
			totalSentimentLong=1;
			valueSentimentLong=sentiment;
		}else{
			totalSentimentLong=1+total.longValue();
			valueSentimentLong=sentiment+current.longValue();
		}
		sentimentMap.put(country, new Long(valueSentimentLong));
		totalMap.put(country, new Long(totalSentimentLong));
		collector.emit(new Values(((double)valueSentimentLong)/((double)totalSentimentLong),country,totalSentimentLong));
		//LOG.info("Sentiment count of "+country+" is "+totalSentimentLong);
		count++;
		LOG.info("count is:"+count);
		if(count==1000){
			LOG.info("TIME is:"+System.currentTimeMillis());
		}
	}
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("countrySentiment","country","total"));
	}
}
