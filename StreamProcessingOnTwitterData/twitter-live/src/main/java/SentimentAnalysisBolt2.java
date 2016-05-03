import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import twitter4j.HashtagEntity;
import twitter4j.Status;

public class SentimentAnalysisBolt2 extends BaseRichBolt{
	private static final Logger LOG = Logger.getLogger(SentimentAnalysisBolt2.class);
	private OutputCollector collector;
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		 this.collector=collector;
	}
	public void execute(Tuple input) {
		Status status=(Status) input.getValueByField("status");
		String tweet=status.getText();
		//String simple=tweet.replaceAll("(RT\\s@\\S+)|(http\\S+)|(https\\S+)|([!.?]{2,})|[-+#@^:…,\\[\\]\\(\\)/&\",]", "");
		
		if(status.getUser().getTimeZone()!=null)
			collector.emit(new Values(status.getUser().getTimeZone()));
		LOG.info("tweet is: "+tweet);
	}
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("timezone"));
	}
}
