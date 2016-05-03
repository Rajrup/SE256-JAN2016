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

public class NormalSentimentAnalysisBolt extends BaseRichBolt{
	private static final Logger LOG = Logger.getLogger(NormalSentimentAnalysisBolt.class);
	private OutputCollector collector;
	private NLP nlp;
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		 this.collector=collector;
		 nlp=new NLP();
	}
	public void execute(Tuple input) {
		String line=(String) input.getValueByField("line");
		int sentiment=nlp.findSentiment(line);
		collector.emit(new Values(sentiment,"Mexico"));
	}
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentiment","timezone"));
	}
}
